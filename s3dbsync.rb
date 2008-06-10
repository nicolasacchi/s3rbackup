require 'rubygems'
require 'aws/s3'
require 'tempfile'
#require 'active_record'

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
		doc["name"] = name
		doc["bucket"] = @config["bucket"]
		doc["datetime"] = Time.now
		doc["current_path"] = `pwd`.gsub("\n","").to_s
		doc["description"] = descr
		doc["host"] = `hostname`.gsub("\n","").to_s
		doc["user"] = `whoami`.gsub("\n","").to_s
		doc["size"] = File.size(tf.path)
		@db << doc
		aws_name = "#{doc["name"]}_#{`date +%Y%m%d_%H.%M.%S`}_#{@db.find_index(doc)}".gsub("\n","")
		doc["aws_name"] = aws_name
		#FIXME Controllare che in db venga salvato aws_name

		store = S3Object.store(aws_name, open(tf.path), @config["bucket"])
		obj = S3Object.find(aws_name, @config["bucket"])
		obj.metadata[:host] = doc["host"]
		obj.metadata[:user] = doc["user"]
		obj.metadata[:descrizione] = doc["description"]
		obj.metadata[:current_path] = doc["current_path"]
		obj.metadata[:size] = doc["size"]
		obj.store
		obj.about.each do |key,val|
			doc[key] = val
		end

		#doc[:about] = obj.about
		#doc += obj.about
		p obj.metadata
		#TODO aggiungere check
		#TODO aggiornare db
	end

	def find(words, bucket = nil)
		option = {}
		words_search = []
		words.each do |word|
			if word =~ /.*=.*/
				#opzione
				option[word.split("=")[0]] = word.split("=")[1]
			else
				words_search << word
			end
		end
		#p option
		#p words_search
		results = []
		@db.each do |item|
			option.each do |key,val|
				if item[key] =~ /#{val}/
					results << item
				end
			end
			words_search.each do |word|
				#p item.values.join(" ")
				if item.values.join(" ") =~ /.*#{word}.*/
					results << item
				end
			end
		end
		results.uniq!
		return results
	end

	def get(aws_name, bucket, out_name, out_dir = nil)
		if out_dir
			`mkdir -p #{out_dir}` if out_dir.end_with?("/")
			out_name = "#{out_dir}#{out_name}"
		end
		open("#{out_name}.tar.bz2", 'w') do |file|
			S3Object.stream(aws_name, bucket) do |chunk|
				file.write chunk
			end
		end
	end

	def unpack(aws_name, bucket, out_name = nil)
		tf = Tempfile.new("s3unbackup")
		open(tf.path, 'w') do |file|
			S3Object.stream(aws_name, bucket) do |chunk|
				file.write chunk
			end
		end
		if out_name
			`mkdir -p #{out_name}`
			`cd out_name`
			tar = `tar xfj #{tf.path}`
			`cd -`
		else
			tar = `tar xfj #{tf.path}`
		end
	end

end

class Configure
	attr_reader :current
	def initialize(file_name = "#{ENV['HOME']}/.s3backup/config.yml")
		file_name = "#{ENV['HOME']}/.s3backup/config.yml" if !file_name
		@current = YAML::load(File.open(file_name))
	end
end

