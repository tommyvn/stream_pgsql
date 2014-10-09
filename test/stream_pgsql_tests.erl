-module(stream_pgsql_tests).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

%% -define(IOModule, file).
-define(IOModule, stream_pgsql).
-define(CHUNK_SIZE, 128).
-define(TEST_FILE, "/tmp/test").

run_all_test_() ->
  { setup,
    fun() ->
      ?IOModule:start()
    end,
    fun(_) ->
      application:stop(stream_pgsql)
    end,
    fun (_) ->
      {inorder,
        [
          delete_existing_file_(),
          delete_non_existant_file_(),
          open_file_with_bad_args_(),
          open_existing_file_write_(),
          open_for_read_and_close_file_(),
          create_and_close_file_(),
          open_read_and_write_to_file_(),
          open_write_and_read_from_file_(),
          open_and_close_and_write_to_file_(),
          open_and_write_to_file_(),
          open_non_existent_file_for_read_(),
          write_read_write_some_more_read_(),
          write_multiple_read_to_eof_read_close_read_(),
          write_and_read_back_file_(),
          write_and_multiple_reads_(),
          read_by_chunk_size_()
%%           somehow_test_pid_dies_on_linked_pid_exit(),
%%           somehow_test_the_chunk_size_is_honered()
        ]
      }
    end
  }.

delete_non_existant_file_() ->
%% delete_non_existant_file_test_() ->
  {setup,
   fun() ->
     Name = ?TEST_FILE,
     ?IOModule:delete(Name),
     Name
   end,
   fun(_Name) ->
     ok
   end,
   fun(Name) ->
     R1 = ?IOModule:delete(Name),
     [
       ?_assertMatch({error, enoent}, R1)
     ]
   end
  }.

open_file_with_bad_args_() ->
  Name = ?TEST_FILE,
  ?_assertEqual({error, badarg}, ?IOModule:open(Name, [read, some_bad_arg])).

open_existing_file_write_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
      ?IOModule:close(IODevice),
      Name
    end,
    fun(Name) ->
      ?IOModule:delete(Name),
      ok
    end,
    fun(Name) ->
      R1 = ?IOModule:open(Name, [write, exclusive, binary]),
      [
        ?_assertMatch({error, eexist}, R1)
      ]
    end
  }.

create_and_close_file_() ->
  {setup,
   fun() ->
     Name = ?TEST_FILE,
     ?IOModule:delete(Name),
     Name
   end,
   fun(Name) ->
     ?IOModule:delete(Name),
     ok
   end,
   fun(Name) ->
     R1 = ?IOModule:open(Name, [write, exclusive, binary]),
     {ok, IODevice} = R1,
     R2 = ?IOModule:close(IODevice),
     R3 = is_process_alive(IODevice),
     [
       ?_assertMatch(ok, R2),
       ?_assertNot(R3)
     ]
   end
  }.

delete_existing_file_() ->
  Name = ?TEST_FILE,
  ?IOModule:delete(Name),
  {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
  ?IOModule:close(IODevice),
  [
    ?_assertMatch(ok, ?IOModule:delete(Name))
  ].

open_for_read_and_close_file_() ->
  {setup,
   fun() ->
     Name = ?TEST_FILE,
     ?IOModule:delete(Name),
     {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
     ?IOModule:close(IODevice),
     Name
   end,
   fun(Name) ->
     ?IOModule:delete(Name),
     ok
   end,
   fun(Name) ->
     R1 = ?IOModule:open(Name, [read, binary]),
     {ok, IODevice} = R1,
     R2 = is_process_alive(IODevice),
     R3 = ?IOModule:close(IODevice),
     R4 = is_process_alive(IODevice),
     [
       ?_assert(R2),
       ?_assertMatch(ok, R3),
       ?_assertNot(R4)
     ]
   end
  }.

open_non_existent_file_for_read_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      Name
    end,
    fun(_Name) ->
      ok
    end,
    fun(Name) ->
      R1 = ?IOModule:open(Name, [read, binary]),
      [
        ?_assertMatch({error, enoent}, R1)
      ]
    end
  }.

open_and_write_to_file_() ->
  {setup,
   fun() ->
     Name = ?TEST_FILE,
     ?IOModule:delete(Name),
     {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
     {Name, IODevice}
   end,
   fun({Name, IODevice}) ->
     ?IOModule:close(IODevice),
     ?IOModule:delete(Name)
   end,
   fun({_Name, IODevice}) ->
     R1 = ?IOModule:write(IODevice, "testing"),
     [
       ?_assertMatch(ok, R1)
     ]
   end
  }.

open_and_close_and_write_to_file_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
      ?IOModule:close(IODevice),
      {Name, IODevice}
    end,
    fun({Name, _IODevice}) ->
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODevice}) ->
      R1 = ?IOModule:write(IODevice, "testing"),
      [
        ?_assertMatch({error, terminated}, R1)
      ]
    end
  }.

open_read_and_write_to_file_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
      ?IOModule:close(IODeviceW),
      {ok, IODevice} = ?IOModule:open(Name, [read, binary]),
      {Name, IODevice}
    end,
    fun({Name, IODevice}) ->
      ?IOModule:close(IODevice),
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODevice}) ->
      R1 = ?IOModule:write(IODevice, "testing"),
      [
        ?_assertMatch({error, ebadf}, R1)
      ]
    end
  }.

open_write_and_read_from_file_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODevice} = ?IOModule:open(Name, [write, exclusive, binary]),
      {Name, IODevice}
    end,
    fun({Name, IODevice}) ->
      ?IOModule:close(IODevice),
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODevice}) ->
      R1 = ?IOModule:read(IODevice, 512),
      R2 = ?IOModule:read(IODevice, 512),
      [
        ?_assertMatch({error,ebadf}, R1),
        ?_assertMatch({error,terminated}, R2)
      ]
    end
  }.

write_and_read_back_file_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      Name
    end,
    fun(Name) ->
      ?IOModule:delete(Name),
      ok
    end,
    fun(Name) ->
      {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
      R1 = ?IOModule:write(IODeviceW, "some data"),
      ?IOModule:close(IODeviceW),
      {ok, IODeviceR} = ?IOModule:open(Name, [read, binary]),
      R2 = ?IOModule:read(IODeviceR, 512),
      R3 = ?IOModule:close(IODeviceR),
      R4 = is_process_alive(IODeviceR),
      [
        ?_assertMatch(ok, R1),
        ?_assertMatch({ok, <<"some data">>}, R2),
        ?_assertNotMatch({ok, <<"some other data">>}, R2),
        ?_assertMatch(ok, R3),
        ?_assertNot(R4)
      ]
    end
  }.

write_read_write_some_more_read_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
      {ok, IODeviceR} = ?IOModule:open(Name, [read, binary]),
      {Name, IODeviceW, IODeviceR}
    end,
    fun({Name, IODeviceW, IODeviceR}) ->
      ?IOModule:close(IODeviceW),
      ?IOModule:close(IODeviceR),
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODeviceW, IODeviceR}) ->
      R1 = ?IOModule:write(IODeviceW, "some data"),
      R2 = ?IOModule:read(IODeviceR, 512),
      R3 = ?IOModule:write(IODeviceW, "some more data"),
      R4 = ?IOModule:read(IODeviceR, 512),
      [
        ?_assertMatch(ok, R1),
        ?_assertMatch({ok, <<"some data">>}, R2),
        ?_assertMatch(ok, R3),
        ?_assertMatch({ok, <<"some more data">>}, R4)
      ]
    end
  }.

write_multiple_read_to_eof_read_close_read_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
      {ok, IODeviceR1} = ?IOModule:open(Name, [read, binary]),
      {ok, IODeviceR2} = ?IOModule:open(Name, [read, binary]),
      {Name, IODeviceW, IODeviceR1, IODeviceR2}
    end,
    fun({Name, _IODeviceW, IODeviceR1, IODeviceR2}) ->
      ?IOModule:close(IODeviceR1),
      ?IOModule:close(IODeviceR2),
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODeviceW, IODeviceR1, IODeviceR2}) ->
      R1 = ?IOModule:write(IODeviceW, "some data"),
      R2 = ?IOModule:read(IODeviceR1, 512),
      R3 = ?IOModule:read(IODeviceR1, 512),
      R4 = ?IOModule:write(IODeviceW, "some data"),
      R5 = ?IOModule:read(IODeviceR1, 512),
      R6 = ?IOModule:read(IODeviceR2, 512),
      R7 = ?IOModule:read(IODeviceR2, 512),
      R8 = ?IOModule:close(IODeviceW),
      R9 = is_process_alive(IODeviceW),
      R10 = ?IOModule:read(IODeviceR1, 512),
      R11 = ?IOModule:read(IODeviceR2, 512),
      [
        ?_assertMatch(ok, R1),
        ?_assertMatch({ok, <<"some data">>}, R2),
        ?_assertMatch({ok, <<>>}, R3),
        ?_assertMatch(ok, R4),
        ?_assertMatch({ok, <<"some data">>}, R5),
        ?_assertMatch({ok, <<"some datasome data">>}, R6),
        ?_assertMatch({ok, <<>>}, R7),
        ?_assertMatch(ok, R8),
        ?_assertNot(R9),
        ?_assertMatch(eof, R10),
        ?_assertMatch(eof, R11)
      ]
    end
  }.

write_and_multiple_reads_() ->
  {setup,
    fun() ->
      Name = ?TEST_FILE,
      ?IOModule:delete(Name),
      {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
      {Name, IODeviceW, lists:map(fun({ok, X}) -> X end, [ ?IOModule:open(Name, [read, binary]) || _ <- lists:seq(0,0) ])}
    end,
    fun({Name, _IODeviceW, IODeviceRList}) ->
      [ ?IOModule:close(IODeviceR) || IODeviceR <- IODeviceRList ],
      ?IOModule:delete(Name)
    end,
    fun({_Name, IODeviceW, [ IODeviceR | _IODeviceRList ]}) ->
      spawn_link(
        fun() ->
          write(IODeviceW, 10),
          ok
        end
      ),
      R1 = read(IODeviceR),
      Expected = list_to_binary([ io_lib:format("some data ~B~n", [C]) || C <- lists:seq(10,1,-1) ]),
      [
        ?_assertEqual({ok, Expected}, R1)
      ]
    end
  }.

write(IODevice, 0) ->
  ?IOModule:close(IODevice);

write(IODevice, Count) ->
  ?IOModule:write(IODevice, io_lib:format("some data ~B~n", [Count])),
  write(IODevice, Count -1).

read(IODeviceR) ->
  read(IODeviceR, <<>>).

read(IODeviceR, Acc) ->
  case ?IOModule:read(IODeviceR, 512) of
    eof ->
      {ok, Acc};
    {ok, <<>>} ->
      timer:sleep(500),
      read(IODeviceR, Acc);
    {ok, Data} ->
      NewAcc = <<Acc/binary, Data/binary>>,
      read(IODeviceR, NewAcc);
    Error ->
      Error
  end.

read_by_chunk_size_() ->
  {setup,
   fun() ->
     Name = ?TEST_FILE,
     ?IOModule:delete(Name),
     {ok, IODeviceW} = ?IOModule:open(Name, [write, exclusive, binary]),
     ?IOModule:write(
       IODeviceW,
       lists:foldl(
         fun(X, Acc) ->
           NewX = list_to_binary(X),
           <<Acc/binary, NewX/binary>>
         end, <<>>, [ integer_to_list(X) || Y <- lists:seq(0,90), X <- lists:seq(0,9) ])),
     ?IOModule:close(IODeviceW),
     {ok, IODeviceR} = ?IOModule:open(Name, [read, binary]),
     {Name, IODeviceR}
   end,
   fun({Name, IODeviceR}) ->
     ?IOModule:close(IODeviceR),
     ?IOModule:delete(Name)
   end,
   fun({_Name, IODeviceR}) ->
     R1 = ?IOModule:read(IODeviceR, 10),
     R2 = {ok, R3} = ?IOModule:read(IODeviceR, 20),
     [
       ?_assertEqual({ok, <<"0123456789">>}, R1),
       ?_assertEqual({ok, <<"01234567890123456789">>}, R2),
       ?_assertEqual(20, size(R3))
     ]
   end
  }.