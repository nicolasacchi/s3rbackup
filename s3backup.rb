require 'rubygems'
require 'aws/s3'
require 'tempfile'
#require 'active_record'
#require "s3dbsync.rb"
require 'optparse'
require 'yaml'
#FIXME creare un db in locale con le informazioni prese dai metadata

class S3SyncDb
	include AWS::S3
	attr :db
	def initialize(config)
		@conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => config["access_key_id"],
			    			:secret_access_key => config["secret_access_key"])

		@config = config
		@db_file = config["db_file"]
		@db_file = "#{ENV['HOME']}/.s3backup/s3db.yml" if !@db_file
		@db_file_ver = "#{@db_file}.ver"
		#apro il db
		if !File.exists?(@db_file)
			#download db
			begin
				buck_db = Bucket.find(config.bucket_db)
			rescue
				#devo crearlo
				if config["sync_db"]
					Bucket.create(config.bucket_db)
				end
				@db = []
				@version = 0
				return
			end
			#lo copio in locale
		  db_file = S3Object.find('s3rbackup_yaml_db', config.bucket_db)
			open(@db_file, 'w') do |file|
				db_file.stream do |chunk|
					file.write chunk
		    end
		  end
			@version = db_file[:version].to_i
		end
		#lo carico
		@db = YAML::load(File.open(@db_file))
		if !@version and config["sync_db"]
			#devo controllare le due versioni...
		end
	end

	def salva_locale
		File.open(@db_file, 'w') { |f| f.puts @db.to_yaml }
	end

	def nuova_versione
		@version += 1
		File.open(@db_file_ver, 'w') { |f| f.puts @version.to_s }
	end

	def aggiornaS3
		nuova_versione
		salva_locale()
		S3Object.store("s3rbackup_yaml_db", open(@db_file, "r"), @config["bucket_db"])
		db_file = S3Object.find('s3rbackup_yaml_db', @config["bucket_db"])
		db_file[:version] = @version.to_s
	end

	def salva_db
		salva_locale()
		aggiornaS3() if @config["sync_db"]
	end

	def crea_bucket(bucket)
		#find bucket
		@bucket = bucket
		begin
			bbackup = Bucket.find(bucket)
		rescue
			if !Bucket.create(bucket)
				raise "Can't create bucket:#{bucket}"
			end
		end
	end	

	def bak(dirs, name, descr)
		crea_bucket(@config["bucket"])
		name = dirs[0] if !name
		tf = Tempfile.new("s3backup")
		tar = `tar -c #{dirs.join(" ")} | bzip2 -9 > #{tf.path}`
		
		doc = {}
		doc[:nome] = name
		doc[:bucket] = @config["bucket"]
		doc[:datetime] = Time.now
		doc[:current_path] = `pwd`.gsub("\n","").to_s
		doc[:description] = descr
		doc[:host] = `hostname`.gsub("\n","").to_s
		doc[:user] = `whoami`.gsub("\n","").to_s
		doc[:size] = File.size(tf.path)
		@db << doc
		aws_name = "#{doc[:nome]}_#{`date +%Y%m%d_%H.%M.%S`}_#{@db.find_index(doc)}".gsub("\n","")
		doc[:aws_name] = aws_name
		#FIXME Controllare che in db venga salvato aws_name

		store = S3Object.store(aws_name, open(tf.path), @config["bucket"])
		obj = S3Object.find(aws_name, @config["bucket"])
		obj.metadata[:host] = doc[:host]
		obj.metadata[:user] = doc[:user]
		obj.metadata[:descrizione] = doc[:description]
		obj.metadata[:current_path] = doc[:current_path]
		obj.metadata[:size] = doc[:size]
		obj.store
		doc[:about] = obj.about
		p obj.metadata
		#TODO aggiungere check
		#TODO aggiornare db
	end

end

class Configure
	attr_reader :current
	def initialize(file_name = "#{ENV['HOME']}/.s3backup/config.yml")
		file_name = "#{ENV['HOME']}/.s3backup/config.yml" if !file_name
		@current = YAML::load(File.open(file_name))
	end
end

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
