spawner
=======
The spawner lets you parallelize anything, using either threads or
processes. All you need is to write the code to run, configure a few values and
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
s = Spawner::Conductor.new('/path/to/config.yml')
1.upto(10) do |i|
  s.add_duty(Proc.new() { puts "Task #{i}" })
end
s.join()
```

More examples [here](/gray-matter/spawner/tree/master/examples).

Using the spawner as a service
------------------------------
The spawner can also run as a standalone service, to which you send jobs
automagically.

```
### Start the spawner service
$ spawner start
# or, for your own configuration
$ spawner start /path/to/config.yml
```

A bit of tuning
---------------
You have the ability to tweak the spawner in various ways, including :
* choosing to run the code in threads or processes
* setting the number of simultaneous running workers

This gem comes with a [template configuration
file](/gray-matter/spawner/blob/master/etc/config.yml).
