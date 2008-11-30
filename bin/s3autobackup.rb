#!/usr/bin/ruby

require 'rubygems'
require 'optparse'

development_lib = File.join(File.dirname(__FILE__), '..', 'lib')
if File.exists? development_lib + '/s3dbsync.rb'
  $LOAD_PATH.unshift(development_lib).uniq!
end
# always look "here" for include files (thanks aktxyz)
#$LOAD_PATH << File.expand_path(File.dirname(__FILE__)) 
require 's3dbsync'


@current = YAML::load(File.open(ARGV[0]))
@current.each do |backup|
	ret = ""
	ret += `date`
	ret += " Backup #{backup["name"]}\n"
	ret += `s3rbackup.rb -n #{backup["name"]} -d #{backup["description"]} --md5 #{backup["dir"]} 2>&1`
	ret += `date`
	ret += "End backup #{backup["name"]}\n"
	puts ret
end
	
