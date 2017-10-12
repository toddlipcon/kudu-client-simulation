#!/usr/bin/env python

import json
import numpy
import random
import sys
import urllib2

class LatencySampler(object):
  def __init__(self, json_input):
      j = json.load(json_input)
      self.m = j[0]['metrics'][0]
      self.cum_counts = numpy.cumsum([int(x) for x in self.m['counts']])

  def take_samples(self, n):
    vals = numpy.random.randint(int(self.m['total_count']), size=n)
    indexes = numpy.searchsorted(self.cum_counts, vals)
    ret = []
    for idx in indexes:
      ret.append(int(self.m['values'][idx + 1]))
    return ret


def main():
  def _grouper(input_list, n = 2):
    for i in xrange(len(input_list) / n):
      yield input_list[i*n:(i+1)*n]

  l = LatencySampler(sys.stdin)
  time = 0
  num_batches = 10000
  num_hosts = 80
  samples = l.take_samples(num_batches * num_hosts)
  for s in _grouper(samples, num_hosts):
    time += max(s)
  print time/num_batches

if __name__ == "__main__":
  main()
