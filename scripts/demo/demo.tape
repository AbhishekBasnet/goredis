Output assets/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1100
Set Height 640
Set Padding 20
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"

Hide
Type "export PS1='$ ' PATH=/vhs/bin:$PATH && rm -rf /tmp/data && mkdir /tmp/data && clear" Enter
Show

Type "goredis -dir /tmp/data 2>/dev/null &" Enter
Sleep 700ms
Type "redis-cli -p 6380" Enter
Sleep 500ms
Type "SET session:42 alice EX 600" Enter
Sleep 400ms
Type "GET session:42" Enter
Sleep 400ms
Type "TTL session:42" Enter
Sleep 400ms
Type "INCR visits" Enter
Type "INCR visits" Enter
Sleep 400ms
Type "RPUSH jobs build test deploy" Enter
Sleep 400ms
Type "LPOP jobs" Enter
Sleep 400ms
Type "GET jobs" Enter
Sleep 900ms
Type "INFO keyspace" Enter
Sleep 900ms
Type "exit" Enter
Sleep 300ms
Type "kill -TERM %1 && wait; clear" Enter
Sleep 800ms
Type "# restarted: the AOF brings everything back" Enter
Type "goredis -dir /tmp/data 2>/dev/null &" Enter
Sleep 700ms
Type "redis-cli -p 6380 MGET session:42 visits" Enter
Sleep 600ms
Type "redis-cli -p 6380 LRANGE jobs 0 -1" Enter
Sleep 600ms
Type "redis-cli -p 6380 TTL session:42" Enter
Sleep 2500ms
