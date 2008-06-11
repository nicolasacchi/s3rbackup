require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'optparse'
require 'yaml'

# always look "here" for include files (thanks aktxyz)
$LOAD_PATH << File.expand_path(File.dirname(__FILE__)) 
require 's3dbsync'


class OptS3rquery
	def self.parse(args)
		options = {}
		opts = OptionParser.new do |opts|
			opts.banner = "Usage: s3query.rb [options] command <parameters> (parameters can be name=test or simply test)"
		
			opts.on("-s", "search words", String, "Search something") do |name|
				options[:op] = "search"
			end
		
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

			opts.on("-l", "--output-cols COLS,COLS...", String, "Column of output") do |name|
				options[:cols] = name.split(",")
			end

			opts.on("-o", "--output-dir DIR", String, "When get or unpack this specifies the output directory") do |name|
				options[:out_dir] = name
			end
	
			opts.on("--last", "When get or unpack this specifies the output directory") do |name|
				options[:last] = true
			end
			#opts.on("-s", "--nosync-db", "Don't sync local db with remote") do |s|
			#	options[:nosync] = s
			#end
		
			opts.on_tail("-h", "--help", "Show this message") do
				puts opts
				exit
			end
		end #.parse!
		opts.parse!(args)
		options
	end
end

options = OptS3rquery.parse(ARGV)
#p options
#in argv rimane tutto il resto
#p ARGV

config = Configure.new(options[:file_cfg])
config.current["bucket"] = options[:bucket] if options[:bucket]

s3db = S3SyncDb.new(config.current)

command = ARGV.shift
case command
	when 'search'
		#cerca
		results = s3db.find(ARGV)
		results.each do |ret|
			if options[:cols]
				outp = []
				options[:cols].each do |col|
					outp << ret[col]
				end
				puts outp.join("\t")
			else
				puts "#{ret["aws_name"]}\t#{ret["description"]}"
			end
		end
	when 'get'
		#scarica
		results = s3db.find(ARGV)
		results.each do |ret|
			puts "Downloading of #{ret["aws_name"]}"
			s3db.get(ret, ret["aws_name"])
		end
	when 'unpack'
		#estrai nella dir
		results = s3db.find(ARGV)
		results.each do |ret|
			puts "Unpacking of #{ret["aws_name"]}"
			s3db.unpack(ret, options[:out_dir])
		end
	when 'delete'
		#cancella
		results = s3db.find(ARGV)
		results.each do |ret|
			puts "Deleting of #{ret["aws_name"]}"
			s3db.delete(ret)
		end
		s3db.salva_db
	else
		puts "Some error occurred command #{command} not valid"
end

