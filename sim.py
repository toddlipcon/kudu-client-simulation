#!/usr/bin/python

import collections
import os
import random
import sample
import simpy

SIM_TIME_SECS = 60

# Num servers to simulate.
NUM_SERVERS = 50

# Max number of buffered ops.
BUFFER_SIZE = 10000

MAX_TO_SEND = 1000

# Latency overhead for each row.
US_PER_ROW = 10

SAMPLE_FILE = os.path.join(os.path.dirname(__file__), "sample-metrics.json")

VERBOSE = False

Response = collections.namedtuple('Response', ['server_idx', 'rows'])

class Client(object):
  def __init__(self, sampler, env):
    self.env = env
    self.buffer_sizes = [0] * NUM_SERVERS
    self.total_buffer = 0
    self.pending = [None] * NUM_SERVERS
    self.pending_results = 0
    self.completed_rows = 0
    self.sampler = sampler
    self._cached_samples = []

  def buffer_random_row(self):
    dst_server = random.randint(0, NUM_SERVERS - 1)
    self.buffer_sizes[dst_server] += 1
    self.total_buffer += 1

  def sample_latency(self, rows_in_batch):
    if not self._cached_samples:
      self._cached_samples = self.sampler.take_samples(BUFFER_SIZE)
    overhead = rows_in_batch * US_PER_ROW
    return self._cached_samples.pop() + overhead

  def send_buffers(self, min_to_send=1, max_to_send=MAX_TO_SEND):
    n_sent = 0
    for dst_server in xrange(NUM_SERVERS):
      if self.buffer_sizes[dst_server] >= min_to_send and \
         self.pending[dst_server] is None:
        self.send_pending_buffer(dst_server, max_to_send)
        n_sent += 1
    return n_sent

  def send_pending_buffer(self, dst_server, max_to_send):
    to_send = min(max_to_send, self.buffer_sizes[dst_server])
    latency = self.sample_latency(to_send)
    p = self.env.timeout(delay=latency, value=[Response(dst_server, to_send)])
    if VERBOSE:
      print "%.3f %d -> TS%d" % (self.env.now / 1e6, to_send, dst_server)
    self.pending[dst_server] = p
    self.pending_results += to_send


  def process_some_responses(self):
    to_wait = [p for p in self.pending if p]
    responses = yield self.env.any_of(to_wait)
    for r_array in responses.values():
      r = r_array[0]
      if VERBOSE:
        print "%.3f %d <- TS%d" % (self.env.now / 1e6, r.rows, r.server_idx)
      self.pending[r.server_idx] = None
      self.buffer_sizes[r.server_idx] -= r.rows
      self.total_buffer -= r.rows
      self.pending_results -= r.rows
      self.completed_rows += r.rows

class CurClient(Client):
  def run(self):
    while True:
      # Fill up to the max buffer size.
      while self.total_buffer < BUFFER_SIZE:
        self.buffer_random_row()
      # Flush everything
      self.send_buffers()
      # Wait until we've received responses from every server.
      while self.pending_results:
        yield self.env.process(self.process_some_responses())


class NewClient1(Client):
  def run(self):
    while True:
      self.buffer_random_row()
      # If we've filled up our buffers, flush to every server
      # that doesn't already have a pending op.
      while self.total_buffer >= BUFFER_SIZE:
        self.send_buffers()
        yield self.env.process(self.process_some_responses())

class NewClient2(Client):
  def run(self):
    send_threshold = BUFFER_SIZE / 5
    while True:
      yield self.env.process(self.process_some_responses())
      while self.total_buffer < BUFFER_SIZE:
        self.buffer_random_row()
      if self.pending_results <= send_threshold:
        avail = self.total_buffer - self.pending_results
        self.send_buffers(min_to_send=avail/NUM_SERVERS)



def main():
  l = sample.LatencySampler(file(SAMPLE_FILE))
  for client_impl in (NewClient2,):
    env = simpy.Environment()
    c = client_impl(l, env)
    env.process(c.run())
    print "time\trows/sec\tavg rows/sec"
    prev = 0
    for sec in xrange(1, SIM_TIME_SECS):
      env.run(until=sec * 1e6)
      delta = c.completed_rows - prev
      prev = c.completed_rows
      print "%3d\t%10d\t%10d" % (env.now/1.0e6, delta, c.completed_rows / sec)


if __name__ == "__main__":
  main()
