PROJECT = stream

DEPS = pgsql uri
dep_pgsql = git https://github.com/epgsql/epgsql.git 2.0.0
dep_uri   = git https://github.com/erlware/uri.git v0.4.0

eunit: app
	rm -fr .eunit/
	mkdir .eunit/
	cp -R test/* .eunit/
	erlc -o .eunit/ .eunit/*.erl
	cp -R ebin/*.beam ebin/*.app deps/*/ebin/*.beam deps/*/ebin/*.app .eunit/
	erl -noshell -pa .eunit -eval "eunit:test(stream_tests, [verbose])" -s init stop

include erlang.mk
