require 'rubygems'
require 'optparse'

# always look "here" for include files (thanks aktxyz)
$LOAD_PATH << File.expand_path(File.dirname(__FILE__)) 
require 's3dbsync'


class OptS3rquery
	def self.parse(args)
		options = {}
		opts = OptionParser.new do |opts|
			opts.banner = "Usage: s3query.rb [options] <search|get|unpack|delete> <parameters> (parameters can be name=test or simply test)"
		
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
	
			opts.on("--newer", "Get only the newest item, with the same name") do |name|
				options[:newer] = true
			end
	
			opts.on("--older", "Get only the oldest item, with the same name") do |name|
				options[:older] = true
			end
	
			opts.on("--last", "Get only the newest item (only one result)") do |name|
				options[:last] = true
			end
	
			opts.on("--first", "Get only the oldest item (only one result)") do |name|
				options[:first] = true
			end

			opts.on("--per-bucket", "Get results grouped per bucket") do |name|
				options[:first] = true
			end

			opts.on("--size", "Get size") do |name|
				options[:first] = true
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

def get_last(res)
	ret = []
	ret << res[res.nitems - 1] if res.nitems > 0
	ret
end

def get_first(res)
	ret = []
	ret << res[0] if res.nitems > 0
	ret
end

options = OptS3rquery.parse(ARGV)
#p options
#in argv rimane tutto il resto
#p ARGV

config = Configure.new(options[:file_cfg])
config.current["bucket"] = options[:bucket] if options[:bucket]

s3db = S3SyncDb.new(config.current)

command = ARGV.shift
results = s3db.find(ARGV, nil, options)
results = get_last(results) if options[:last]
results = get_first(results) if options[:first]
case command
	when 'search'
		#cerca
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
		results.each do |ret|
			puts "Downloading of #{ret["aws_name"]}"
			s3db.get(ret, ret["aws_name"])
		end
	when 'unpack'
		#estrai nella dir
		results.each do |ret|
			puts "Unpacking of #{ret["aws_name"]}"
			s3db.unpack(ret, options[:out_dir])
		end
	when 'delete'
		#cancella
		results.each do |ret|
			puts "Deleting of #{ret["aws_name"]}"
			s3db.delete(ret)
		end
		s3db.salva_db
	when 'stats'
		bucks_s = {}
		results.each do |ret|
			bucks_s[ret["bucket"]] ||= 0
			bucks_s[ret["bucket"]] += ret["size"]
		end
		bucks_s.each do |key,val|
			puts "#{key}:\t#{sprintf("%.2fMb", val / (1024.0 * 1024.0))}"
		end
	else
		puts "Some error occurred command #{command} not valid"
end

