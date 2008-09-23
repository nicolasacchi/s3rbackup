#!/usr/bin/ruby

require 'rubygems'
require 'optparse'

development_lib = File.join(File.dirname(__FILE__), '..', 'lib')
if File.exists? development_lib + '/s3dbsync.rb'
  $LOAD_PATH.unshift(development_lib).uniq!
end
require 's3dbsync'


class OptS3rbackup
	def self.parse(args)
		options = {}
		opts = OptionParser.new do |opts|
			opts.banner = "Usage: s3backup.rb [options] <files>"
		
			opts.on("-n", "--name [NAME]", String, "Backup name") do |name|
				options[:name] = name
			end
		
			opts.on("-b", "--bucket BUCKET", String, "Bucket name") do |bucket|
				options[:bucket] = bucket
			end
		
			opts.on("-d", "--description DESCRIPTION", String, "Description") do |name|
				options[:descr] = name
			end
			
			opts.on("-c", "--file-cfg PATH", String, "Path of cfg file") do |name|
				options[:file_cfg] = name
			end
			
			opts.on("-s", "--nosync-db", "Don't sync local db with remote") do |s|
				options[:nosync] = s
			end
		
			opts.on("--log", "Log enabled") do |name|
				options[:log] = true
			end
		
			opts.on("--nolog", "Log disabled") do |name|
				options[:log] = false
			end

			opts.on("--bucket-log NAME", String, "Bucket log NAME") do |name|
				options[:bucket_log] = name
			end

			opts.on("-p", "--compression [bz2|lzma|7z|gz]", String, "Compression type (always at maximum compression)") do |name|
				options[:compression] = name
			end

			opts.on("-u", "--config-number NUM", Integer, "Number of config to use if nil use first") do |name|
				options[:config_num] = name
			end

			opts.on_tail("-h", "--help", "Show this message") do
				puts opts
				exit
			end
		end #.parse!
		opts.parse!(args)
		options
	end
end

options = OptS3rbackup.parse(ARGV)
#p options
#in argv rimane tutto il resto
#p ARGV

config = Configure.new(options[:file_cfg], options[:config_num])
config.current["bucket"] = options[:bucket] if options[:bucket]
config.current["log"] = options[:log] if options[:log] != nil
config.current["bucket_log"] = options[:bucket_log] if options[:bucket_log]
config.current["compression"] = options[:compression] if options[:compression]
s3db = S3SyncDb.new(config.current)
s3db.bak(ARGV,  options[:name],  options[:descr])
#s3db.salva_db
