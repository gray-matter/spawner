spawner
=======
/!\ This is still a WIP project, release coming soon /!\

The spawner lets you parallelize anything, using either threads or processes.
You may instantiate it via the Ruby library or start a spawner service to
which you may send tasks via the command line.

All you need is to write the code to run, configure a few values and
let the magic happen !

Installation
------------
```
gem install spawner
```

Using the spawner as a class
----------------------------
```ruby
require 'spawner'

EXPECTED_RETURN = 0

s = Spawner::Conductor.new()
# Note: load_config_from_hash is also available
s.load_config_from_file('/path/to/config.yml')

1.upto(10) do |i|
  s.add_duty(EXPECTED_RETURN) do
    puts "Task #{i}"
    0
  end
end

s.join()
```

More examples [here](/examples).

Using the spawner as a service
------------------------------
The spawner can also run as a standalone service, to which you send jobs
which will be executed automagically.

Starting the service is as simple as:
```
### Start the spawner service (the configuration is optional)
$ spawner start /path/to/config.yml
```

There are two ways to give it work to do
```
### Run a shell command (this will simply exec the command)
$ spawner exec 'echo "I'm a potato"'

### Execute Ruby code
$ spawner run 'puts "I'm a potato"'
```

The spawner service also supports stop/reload/restart as a regular service
would.

A bit of tuning
---------------
You have the ability to tweak the spawner in various ways, including :
* choosing to run the code in threads or processes
* setting the number of simultaneous running workers

This gem comes with a [template configuration file](/etc/config.yml).
