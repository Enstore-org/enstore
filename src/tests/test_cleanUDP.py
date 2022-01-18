# udp_common includes C module imports.
# If this import fails, please read enstore-pytest-c-module.md,
# and remember to use `python -m pytest`.
import cleanUDP
import pprint
import os
import pytest
import select
from mock import patch
from cleanUDP import *

def test_Select():
  # This calls Python's select.select and gets some objects. It calls scrub() on each of them to clean them, which is a method defined on `cleanUDP` class, so I'm guessing there is an assumption that all the objects returned by select.select are `cleanUDP`s.
  pass


class TestCleanUDP:

  @pytest.fixture(params=[socket.AF_INET, socket.AF_INET6])
  def udp_clean_udp_pair(self, request):
    clean_udp = cleanUDP(request.param, socket.SOCK_DGRAM)
    clean_udp.bind(clean_udp.getsockname())
    udp = socket.socket(request.param, type=socket.SOCK_DGRAM)
    udp.bind(udp.getsockname())
    yield (clean_udp, udp)
    clean_udp.close()
    udp.close()

  def test_init(self, udp_clean_udp_pair):
    for udp_socket in udp_clean_udp_pair:
      port = udp_socket.getsockname()[1]
      assert port != 0
      assert udp_socket.type == socket.SOCK_DGRAM

  def test_scrub(self, udp_clean_udp_pair):
    clean_udp, udp = udp_clean_udp_pair
    # Nothing on socket, should return 0
    assert clean_udp.scrub() == 0
    clean_udp.sendto(b'', clean_udp.getsockname())
    # Read on socket, should return 1
    assert clean_udp.scrub() == 1
    clean_udp.recvfrom(1)
    # Read received, should return 0
    assert clean_udp.scrub() == 0
    clean_udp.connect(udp.getsockname())
    udp.close()
    clean_udp.send(b'')
    # Error on socket, should clear error and return 0
    scrub = clean_udp.scrub()
    error = clean_udp.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    assert scrub == error == 0

  def test_sendto(self, udp_clean_udp_pair):
    clean_udp, udp = udp_clean_udp_pair
    clean_udp.sendto(b'asd', udp.getsockname())
