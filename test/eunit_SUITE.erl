-module(eunit_SUITE).
-compile(export_all).

all() ->
  [eunit].

eunit(_) ->
  %%1 = 1.
  ok = eunit:test(stream_tests).
