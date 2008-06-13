require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'yaml'

class S3SyncDb
	include AWS::S3
	attr :db
	def initialize(config)
		@conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => config["access_key_id"],
			    			:secret_access_key => config["secret_access_key"])

		@config = config
		@db_file = config["db_file"]
		@db_file = "#{ENV['HOME']}/.s3rbackup/s3db.yml" if !@db_file
		@db_file_ver = "#{@db_file}.ver"
		#apro il db
		if !File.exists?(@db_file)
			#download db
			begin
				buck_db = Bucket.find(config["bucket_db"])
			rescue
				#devo crearlo
				if config["sync_db"]
					Bucket.create(config["bucket_db"])
				end
				@db = []
				@version = 0
				return
			end
			#lo copio in locale
		  db_file = S3Object.find('s3rbackup_yaml_db', config["bucket_db"])
			open(@db_file, 'w') do |file|
				S3Object.stream('s3rbackup_yaml_db', config["bucket_db"]) do |chunk|
					file.write chunk
		    end
		  end
			@version = db_file.metadata[:version].to_i
			File.open(@db_file_ver, 'w') { |f| f.puts @version.to_s }
		elsif config["sync_db"]
			begin
				buck_db = Bucket.find(config["bucket_db"])
				db_file = S3Object.find('s3rbackup_yaml_db', config["bucket_db"])
			rescue
				#devo crearlo
				if config["sync_db"]
					Bucket.create(config["bucket_db"])
				end
				@db = YAML::load(File.open(@db_file))
				@version ||= File.read(@db_file_ver).to_i
				return
			end
			#se esiste e devo fare il sync
			@version = db_file.metadata[:version].to_i
			local_ver = File.read(@db_file_ver)
			if @version > local_ver.to_i
				#uso il remoto
				open(@db_file, 'w') do |file|
					db_file.stream do |chunk|
						file.write chunk
			    end
			  end
				File.open(@db_file_ver, 'w') { |f| f.puts @version.to_s }
			else
				#posso usare quello locale
			end
		end
		#lo carico
		@db = YAML::load(File.open(@db_file))
		@version ||= File.read(@db_file_ver).to_i
	end

	def salva_locale
		File.open(@db_file, 'w') { |f| f.puts @db.to_yaml }
	end

	def nuova_versione
		@version += 1
		File.open(@db_file_ver, 'w') { |f| f.puts @version.to_s }
	end

	def aggiornaS3
		salva_locale()
		S3Object.store("s3rbackup_yaml_db", open(@db_file, "r"), @config["bucket_db"])
		db_file = S3Object.find('s3rbackup_yaml_db', @config["bucket_db"])
		db_file.metadata[:version] = @version.to_s
		db_file.store
	end

	def salva_db
		salva_locale()
		nuova_versione()
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
		tf = Tempfile.new("s3rbackup")
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
		doc["compression"] = "bz2"
		doc["archive"] = "tar"
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
		obj.metadata[:compression] = doc["compression"]
		obj.metadata[:archive] = doc["archive"]
		obj.store
		obj.about.each do |key,val|
			doc[key] = val
		end
		#TODO aggiungere check
		#TODO aggiornare db
	end

	def find(words, bucket = nil, cmd_opt = {})
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
		results = []
		@db.each do |item|
			option.each do |key,val|
				if item[key] =~ /#{val}/
					results << item
				end
			end
			words_search.each do |word|
				if item.values.join(" ") =~ /.*#{word}.*/
					results << item
				end
			end
		end
		results.uniq!
		hres = {}
		if cmd_opt[:newer] or cmd_opt[:older]
			results.each do |item|
				results.each do |item2|
					if item["name"] == item2["name"]
						hres[item["name"]] ||= []
						hres[item["name"]] << item
						#hres[:name].sort! {|x,y| x["date"] <=> y["date"]}
					end
				end
			end
			results = []
			#p hres
			hres.each do |key,val|
				val.sort! {|x,y| x["date"] <=> y["date"]}
				results << val[val.nitems - 1] if cmd_opt[:newer]
				results << val[0] if cmd_opt[:older]
			end
		end
		results.sort! {|x,y| x["date"] <=> y["date"]}

		return results
	end

	def get(item, out_name, out_dir = nil)
		aws_name = item["aws_name"]
		bucket = item["bucket"]
		ext = (item["archive"] and item["archive"] != "" ? ".#{item["archive"]}" : "")
		ext += (item["compression"] and item["compression"] != "" ? ".#{item["compression"]}" : "")
		if out_dir
			`mkdir -p #{out_dir}` if out_dir.end_with?("/")
			out_name = "#{out_dir}#{out_name}"
		end
		open("#{out_name}#{ext}", 'w') do |file|
			S3Object.stream(aws_name, bucket) do |chunk|
				file.write chunk
			end
		end
	end

	def unpack(item, out_name = nil)
		aws_name = item["aws_name"]
		bucket = item["bucket"]
		tf = Tempfile.new("s3runbackup")
		open(tf.path, 'w') do |file|
			S3Object.stream(aws_name, bucket) do |chunk|
				file.write chunk
			end
		end
		if out_name
			`mkdir -p #{out_name}`
			tar = `tar --directory #{out_name} -xjf #{tf.path}`
		else
			tar = `tar xfj #{tf.path}`
		end
	end

	def delete(item)
		S3Object.delete item["aws_name"], item["bucket"]
		@db.delete(item)
	end
end

class Configure
	attr_reader :current
	def initialize(file_name = "#{ENV['HOME']}/.s3rbackup/config.yml")
		file_name = "#{ENV['HOME']}/.s3rbackup/config.yml" if !file_name
		@current = YAML::load(File.open(file_name))
	end
end
