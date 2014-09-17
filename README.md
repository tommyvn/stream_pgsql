stream
======

Live streaming file-like API for Erlang

*This isn't yet working.*
Well, it is, but it'll only work if you use it on one node and the backing store is coupled to the API.
But some day soon it will be awesome.


```
1> stream_pgsql:start_all().
ok
2> {ok, WS} = stream_pgsql:open("testing", [exclusive, write, binary]).
{ok,<0.44.0>}
3> {ok, RS1} = stream_pgsql:open("testing", [read, binary]).
{ok,<0.47.0>}
4> {ok, RS2} = stream_pgsql:open("testing", [read, binary]).
{ok,<0.50.0>}
5> stream_pgsql:read(RS1, 512).
{ok,<<>>}
6> stream_pgsql:write(WS, <<"tom was here">>).
ok
7> stream_pgsql:read(RS1, 512).
{ok,<<"tom was here">>}
8> stream_pgsql:read(RS2, 512).
{ok,<<"tom was here">>}
9> stream_pgsql:read(RS2, 512).
{ok,<<>>}
10> stream_pgsql:close(WS).
ok
11> stream_pgsql:read(RS2, 512).
eof
12> stream_pgsql:read(RS1, 512).
eof
13>
```
