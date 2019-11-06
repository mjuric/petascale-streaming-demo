#!/usr/bin/env python

import sys, io, time, argparse, string, signal, itertools, os.path
import json, base64
import fastavro, fastavro.write
from munch import munchify

from confluent_kafka import Consumer, KafkaError, TopicPartition
from contextlib import contextmanager
from collections import namedtuple

from multiprocessing import Pool

# FIXME: Make this into a proper class (safety in the unlikely case the user returns HEARTBEAT_SENTINEL)
HEARTBEAT_SENTINEL = "__heartbeat__"
def is_heartbeat(msg):
	return isinstance(msg, str) and msg == HEARTBEAT_SENTINEL

assert is_heartbeat(HEARTBEAT_SENTINEL)

def _noop(msg):
	return msg

class ParseAndFilter:
	def __init__(self, filter):
		self.filter = filter if filter is not None else _noop

	def __call__(self, msg):
		topic, part, offs, val = msg

		with io.BytesIO(val) as fp:
			rr = fastavro.reader(fp)
			for record in rr:
				record = munchify(record)
				return topic, part, offs, self.filter(record)

def parse_kafka_url(val, allow_no_topic=False):
	assert val.startswith("kafka://")

	val = val[len("kafka://"):]

	try:
		(groupid_brokers, topics) = val.split('/')
	except ValueError:
		if not allow_no_topic:
			raise argparse.ArgumentError(self, f'A kafka:// url must be of the form kafka://[groupid@]broker[,broker2[,...]]/topicspec[,topicspec[,...]].')
		else:
			groupid_brokers, topics = val, None

	try:
		(groupid, brokers) = groupid_brokers.split('@')
	except ValueError:
		(groupid, brokers) = (None, groupid_brokers)

	topics = topics.split(',') if topics is not None else []
	
	return (groupid, brokers, topics)


def open(*args, **kwargs):
	return AlertBroker(*args, **kwargs)

class AlertBroker:
	c = None

	def __init__(self, broker_url, start_at='latest'):
		self.groupid, self.brokers, self.topics = parse_kafka_url(broker_url)

		if self.groupid is None:
			# if groupid hasn't been given, emulate a low-level consumer:
			#   - generate a random groupid
			#   - disable committing (so we never commit the random groupid)
			#   - start reading from the earliest (smallest) offset
			import getpass, random
			self.groupid = getpass.getuser() + '-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
			print(f"Generated fake groupid {self.groupid}")

		self.c = Consumer({
			'bootstrap.servers': self.brokers,
			'group.id': self.groupid,
			'default.topic.config': {
				'auto.offset.reset': start_at
			},
			'enable.auto.commit': False,
			'queued.min.messages': 1000,
		})

		self.c.subscribe(self.topics)

		self._buffer = {}		# local raw message buffer

		self._idx = 0

		self._consumed = {}		# locally consumed (not necessarily committe)
		self._committed = {}	# locally committed, but not yet committed on kafka
		self.last_commit_time = None

	# context manager protocol
	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.close()

	def close(self):
		if self.c:
			self._commit_to_kafka(defer=False)
			self.c.unsubscribe()
			self.c.close()
			self.c = None

	def _raw_stream(self, timeout):
		last_msg_time = time.time()
		while True:
			msgs = self.c.consume(1000, 1.0)
			if not msgs:
				if timeout and (time.time() - last_msg_time >= timeout):
					return
				yield HEARTBEAT_SENTINEL
			else:
				# check for errors
				for msg in msgs:
					if msg.error() is None:
						continue

					if msg.error().code() == KafkaError._PARTITION_EOF:
						continue

					raise Exception(msg.error())
				yield msgs

				# reset the timeout counter
				last_msg_time = time.time()

			# actualy commit any offsets the user committed
			self._commit_to_kafka(defer=True)

	# returns a generator returning deserialized, user-processed, messages + heartbeats
	def _filtered_stream(self, mapper, filter, timeout):
		t_last = time.time()

		for msgs in itertools.chain([self._buffer.values()], self._raw_stream(timeout=timeout)):
			if is_heartbeat(msgs):
				yield None, msgs
			else:
				self._buffer = dict( enumerate(msgs) )

				# unpack and copy so we don't pickle the world (if multiprocessing)
				msgs = [ (msg.topic(), msg.partition(), msg.offset(), msg.value()) for msg in msgs ]

				# process the messages on the workers
				for i, (topic, part, offs, rec) in enumerate(mapper(ParseAndFilter(filter), msgs)):
					# pop the message from the buffer, indicating we've processed it
					del self._buffer[i]

					# mark as consumed and increment the index _before_ we yield,
					# as we may never come back from yielding (if the user decides
					# to break from the loop)
					self._consumed[(topic, part)] = offs
					self._idx += 1

					# yield if the filter didn't return None
					if rec is not None:
						yield self._idx-1, rec

	def commit(self, defer=True):
		self._committed = self._consumed.copy()
		self._commit_to_kafka(defer=defer)

	def _commit_to_kafka(self, defer=True):
		# Occasionally commit read offsets (note: while this ensures we won't lose any
		# messages, some may be duplicated if the program is killed before offsets are committed).
		now = time.time()
		last = self.last_commit_time if self.last_commit_time is not None else 0
		if self._committed and (not defer or now - last > 5.):
			print("COMMITTING", file=sys.stderr)
			tp = [ TopicPartition(_topic, _part, _offs + 1) for (_topic, _part), _offs in self._committed.items() ]
			#print("   TP=", tp)
			self.c.commit(offsets=tp)

			# drop all _committd offsets from _consumed; no need to commit them again if the user
			# calls commit()
			self._consumed = {k:v for k,v in self._consumed.items() if k not in self._committed}
			self._committed = {}

			self.last_commit_time = now

	def _stream(self, filter, mapper, progress, timeout, limit):
		import warnings
		with warnings.catch_warnings():
			# hide the annoying 'TqdmExperimentalWarning' warning
			warnings.simplefilter("ignore")
			from tqdm.autonotebook import tqdm

		t = tqdm(disable=not progress, total=limit, desc='Alerts processed', unit=' alerts', mininterval=0.5, smoothing=0, miniters=0)

		nread = 0
		for idx, rec in self._filtered_stream(mapper=mapper, filter=filter, timeout=timeout):
			if is_heartbeat(rec):
				t.update(0)
				continue

			yield idx, rec

			t.update()
			nread += 1

			if nread == limit:
				break

		t.close()

	# returns a generator for the user-mapped messages using the filter function,
	# possibly executed on ncores and up to maxread values, + heartbeats
	def __call__(self, filter=None, pool=None, progress=False, timeout=None, limit=None):
		if pool:
			mapper = lambda fun, vec: pool.imap(fun, vec, chunksize=50)
		else:
			mapper = map

		yield from self._stream(filter, mapper=mapper, progress=progress, timeout=timeout, limit=limit)

	def __iter__(self):
		return self.__call__()

@contextmanager
def NicePool(*args, **kwarg):
	original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
	p = Pool(*args, **kwarg)
	signal.signal(signal.SIGINT, original_sigint_handler)
	try:
		yield p
	finally:
		p.close()

if __name__ == "__main__":
	def my_filter(msg):
		#return msg
		return None if msg.candidate.ssnamenr == 'null' else msg

	try:
		from datetime import datetime
		with NicePool(5) as workers:
			with AlertBroker("kafka://broker0.do.alerts.wtf/test6", start_at="earliest") as stream:
				for nread, (idx, rec) in enumerate(stream(filter=my_filter, pool=workers, progress=True, timeout=10), start=1):

					## do stuff
					cd = rec.candidate
					print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)

					stream.commit()
				stream.commit(defer=False)

		# with AlertBroker("kafka://broker0.do.alerts.wtf/test8", start_at="earliest") as stream:
		# 	for nread, (idx, rec) in enumerate(stream(progress=True, timeout=2), start=1):

		# 		## do stuff
		# 		cd = rec.candidate
		# 		print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)

		# 		stream.commit()

		# 		if nread == 10:
		# 			break

		# 	print("OUTSEDE")
		# 	print("GOING IN")

		# 	for nread, (idx, rec) in enumerate(stream(progress=True, timeout=10), start=1):
		# 		cd = rec.candidate
		# 		print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)
		# 		stream.commit()

			stream.commit(defer=False)
	except KeyboardInterrupt:
		pass

#################


# def tqdm_stream(*args, **kwargs):
# 	from tqdm import tqdm
# 	return TqdmStream(tqdm, stream(*args, **kwargs))

# 	def __del__(self):
# 		print("LEAVING c", file=sys.stderr)
# 		if c is not None:
# 			# Make sure the consumer is closed (some versions of confluent_kafka segfault if c.close() isn't called
# 			# explicitly).
# 			c.close()
# 		print("CLOSED c", file=sys.stderr)


# @contextmanager
# def KafkaDataSource(groupid, brokers, topics, heartbeat=False, start_at='latest'):
# 	if groupid is None:
# 		# if groupid hasn't been given, emulate a low-level consumer:
# 		#   - generate a random groupid
# 		#   - disable committing (so we never commit the random groupid)
# 		#   - start reading from the earliest (smallest) offset
# 		import getpass, random
# 		groupid = getpass.getuser() + '-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
# ##		print(f"Generated fake groupid {groupid}")

# 	c = None
# 	try:
# 		c = Consumer({
# 			'bootstrap.servers': brokers,
# 			'group.id': groupid,
# 			'default.topic.config': {
# 				'auto.offset.reset': start_at
# 			},
# 			'enable.auto.commit': False,
# 			'queued.min.messages': 1000,
# 		})
# 		c.subscribe(topics)

# 		def read():
# 			t = time.time()
# 			while True:
# 				msgs = c.consume(1000, 1.0)
# 				if not msgs:
# 					if heartbeat:
# 						yield HEARTBEAT_SENTINEL
# 				else:
# 					# check for errors
# 					for msg in msgs:
# 						if msg.error() is None:
# 							continue

# 						if msg.error().code() == KafkaError._PARTITION_EOF:
# 							continue

# 						raise Exception(msg.error())
# 					yield msgs

# 		yield (c, read())
# 	finally:
# 		print("LEAVING c", file=sys.stderr)
# 		if c is not None:
# 			# Make sure the consumer is closed (some versions of
# 			# confluent_kafka segfault if c.close() isn't called
# 			# explicitly).
# 			c.close()
# 		print("CLOSED c", file=sys.stderr)


# def _noop(msg):
# 	return msg

# class ParseAndFilter:
# 	def __init__(self, filter):
# 		self.filter = filter if filter is not None else _noop

# 	def __call__(self, msg):
# 		topic, part, offs, val = msg

# 		with io.BytesIO(val) as fp:
# 			rr = fastavro.reader(fp)
# 			for record in rr:
# 				record = munchify(record)
# 				return topic, part, offs, self.filter(record)

# def parse_kafka_url(val, allow_no_topic=False):
# 	assert val.startswith("kafka://")

# 	val = val[len("kafka://"):]

# 	try:
# 		(groupid_brokers, topics) = val.split('/')
# 	except ValueError:
# 		if not allow_no_topic:
# 			raise argparse.ArgumentError(self, f'A kafka:// url must be of the form kafka://[groupid@]broker[,broker2[,...]]/topicspec[,topicspec[,...]].')
# 		else:
# 			groupid_brokers, topics = val, None

# 	try:
# 		(groupid, brokers) = groupid_brokers.split('@')
# 	except ValueError:
# 		(groupid, brokers) = (None, groupid_brokers)

# 	topics = topics.split(',') if topics is not None else []
	
# 	return (groupid, brokers, topics)

# def kafka_url_str(groupid, brokers, topics):
# 	return "kafka://%s%s/%s" % (groupid + '@' if groupid is not None else '', brokers, ','.join(topics))


# def stream(broker_url, filter=None, start_at='earliest'):

# 	groupid, broker, topics = parse_kafka_url(broker_url)

# 	def _do_stream(c, stream, imap, commit, ncores, maxread):
# 		finished = {}
# 		idx = idx_last = 0
# 		t_last = time.time()
# 		try:
# 			for msgs in stream:
# 				if is_heartbeat(msgs):
# 					yield None, msgs
# 				else:
# 					# unpack and copy so we don't pickle the world
# 					msgs = [ (msg.topic(), msg.partition(), msg.offset(), msg.value()) for msg in msgs ]

# 					# process the messages on the workers
# 					for topic, part, offs, rec in imap(ParseAndFilter(filter), msgs):
# 						# "action"
# 						if rec is not None:
# 							yield idx, rec

# 						finished[(topic, part)] = offs
# 						idx += 1

# 						if idx == maxread:
# 							return

# 				# Occasionally commit read offsets (note: while this ensures we won't lose any
# 				# messages, some may be duplicated if the program is killed before offsets are committed).
# 				now = time.time()
# 				if commit and (now - t_last > 2. or idx - idx_last > 500) and finished:
# 					print("COMMITTING", file=sys.stderr)
# 					tp = [ TopicPartition(_topic, _part, _offs + 1) for (_topic, _part), _offs in finished.items() ]
# 					c.commit(offsets=tp)
# 					t_last, idx_last, finished = now, idx, {}
# 		finally:
# 			if commit and finished:
# 				print("FINAL COMMITTING", file=sys.stderr)
# 				tp = [ TopicPartition(_topic, _part, _offs + 1) for (_topic, _part), _offs in finished.items() ]
# 				c.commit(offsets=tp)

# 	class Stream:
# 		def consume(self, ncores=1, maxread=None, heartbeat=False):
# 			commit = groupid is not None
# 			with KafkaDataSource(groupid, broker, topics, heartbeat, start_at=start_at) as (c, stream):
# 				if ncores == 1:
# 					# single-threaded
# 					yield from _do_stream(c, stream, map, commit, ncores, maxread)
# 				else:
# 					# multiprocessing
# 					# Hack from https://stackoverflow.com/questions/11312525/catch-ctrlc-sigint-and-exit-multiprocesses-gracefully-in-python
# 					# to mask CTRL-C signal reception in chilren (so interruptions in Jupyter look sane)
# 					original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
# 					_map = lambda fun, vec: pool.imap(fun, vec, chunksize=50)
# 					with Pool(ncores) as pool:
# 						signal.signal(signal.SIGINT, original_sigint_handler)
# 						yield from _do_stream(c, stream, _map, commit, ncores, maxread)

# 	return Stream()

# def spinning_cursor():
# 	while True:
# 		for cursor in '|/-\\':
# 			yield cursor

# def progress_stream(*args, **kwargs):
# 	kwargs['heartbeat'] = True
# 	spinner = spinning_cursor()
# 	for idx, rec in stream(*args, **kwargs):
# 		sys.stderr.write('\b')
# 		if is_heartbeat(rec):
# 			sys.stderr.write(next(spinner))
# 			sys.stderr.flush()
# 			continue

# 		yield idx, rec

# class TqdmStream():
# 	def __init__(self, tqdm, inner_stream):
# 		self.tqdm = tqdm
# 		self.inner_stream = inner_stream

# 	def consume(self, *args, **kwargs):
# 		kwargs['heartbeat'] = True

# 		t = self.tqdm(desc='Alerts processed', unit=' alerts', mininterval=0.5, smoothing=1, miniters=0)

# 		for idx, rec in self.inner_stream.consume(*args, **kwargs):
# 			if is_heartbeat(rec):
# 				t.update(0)
# 				continue

# 			yield idx, rec

# 			t.update()

# 		t.close()

# def tqdm_stream(*args, **kwargs):
# 	from tqdm import tqdm
# 	return TqdmStream(tqdm, stream(*args, **kwargs))

# def jupyter_stream(*args, **kwargs):
# 	from tqdm import tqdm_notebook
# 	return TqdmStream(tqdm_notebook, stream(*args, **kwargs))

# def jupyter_stream__(*args, **kwargs):
# 	kwargs['heartbeat'] = True

# 	from tqdm import tqdm_notebook as tqdm
# 	t = tqdm(desc='Alerts processed', unit=' alerts', mininterval=0.5, smoothing=1, miniters=0)

# 	try:
# 		for idx, rec in stream(*args, **kwargs):
# 			if is_heartbeat(rec):
# 				t.update(0)
# 				continue

# 			yield idx, rec

# 			t.update()
# 	except KeyboardInterrupt:
# 		pass
# 	finally:
# 		print("t.CLOSE")
# 		t.close()
# 	print("EXIT", file=sys.stderr)


# from functools import wraps
# def yield_for_change(widget, attribute):
#     """Pause a generator to wait for a widget change event.

#     This is a decorator for a generator function which pauses the generator on yield
#     until the given widget attribute changes. The new value of the attribute is
#     sent to the generator and is the value of the yield.
#     """
#     def f(iterator):
#         @wraps(iterator)
#         def inner():
#             i = iterator()
#             def next_i(change):
#                 try:
#                     i.send(change.new)
#                 except StopIteration as e:
#                     widget.unobserve(next_i, attribute)
#             widget.observe(next_i, attribute)
#             # start the generator
#             next(i)
#         return inner
#     return f

# def jupyter_streamX(*args, **kwargs):
# 	def _jupyter_stream_thread(ctl):
# 		with ctl.output:
# 			kwargs['heartbeat'] = True

# 			from tqdm import tqdm_notebook as tqdm
# 			t = tqdm(desc='Alerts processed', unit=' alerts', mininterval=0.5, smoothing=1, miniters=0)

# #			for idx, rec in stream(*args, **kwargs):
# 			for idx, rec in enumerate(range(1000)):
# 				time.sleep(0.2)
# 				if is_heartbeat(rec):
# 					t.update(0)
# 				else:
# 					yield idx, rec
# 					t.update(1)

# 				# check the controls
# 				while ctl.state == 'pause': time.sleep(0.2)
# 				if ctl.state == 'stop': break

# 			t.close()

# 	class RunController:
# 		def __init__(self):
# 			self.state = 'play'

# 			from IPython.display import display
# 			import ipywidgets as widgets

# 			self.buttons = widgets.ToggleButtons(
# 				options=['Play  ', 'Pause  ', 'Stop  '],
# 				disabled=False,
# 				button_style='', # 'success', 'info', 'warning', 'danger' or ''
# 				tooltips=['Play', 'Pause', 'Stop'],
# 				icons=['play', 'pause', 'stop']
# 			)
# 			self.buttons.observe(self.on_click, 'value')

# 			# Start the worker in a separate thread
# 			self.output = widgets.Output()
# 			display(widgets.VBox([self.output, self.buttons]))

# 		def on_click(self, value):
# 			print("HERE!")
# 			v = value['new']
# 			if v.startswith('Play'):
# 				self.state = 'play'
# 			elif v.startswith('Pause'):
# 				self.state = 'pause'
# 			elif v.startswith('Stop'):
# 				self.state = 'stop'
# 				self.thread.join()

# 	ctl = RunController()
# 	return _jupyter_stream_thread(ctl)

# if __name__ == "__main__":
# 	from datetime import datetime

# 	def my_filter(msg):
# 		return msg
# 		#return None if msg.candidate.ssnamenr == 'null' else msg

# 	strm = tqdm_stream("kafka://broker0.do.alerts.wtf/test8", my_filter, heartbeat=True, ncores=1, start_at='earliest')
# 	for nread, (idx, rec) in enumerate(strm):
# 		# Act on received information
# 		cd = rec.candidate
# 		#print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)
