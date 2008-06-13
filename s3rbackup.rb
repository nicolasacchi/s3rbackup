require 'rubygems'
require 'optparse'

# always look "here" for include files (thanks aktxyz)
$LOAD_PATH << File.expand_path(File.dirname(__FILE__)) 
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

config = Configure.new(options[:file_cfg])
config.current["bucket"] = options[:bucket] if options[:bucket]
s3db = S3SyncDb.new(config.current)
s3db.bak(ARGV,  options[:name],  options[:descr])
s3db.salva_db
