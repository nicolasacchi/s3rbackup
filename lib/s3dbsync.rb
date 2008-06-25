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
		@db_file = "#{ENV['HOME']}/.s3rbackup/#{config['db_file']}"
		@db_file = "#{ENV['HOME']}/.s3rbackup/s3db.yml" if !@db_file
		@db_file_ver = "#{@db_file}.ver"
		#apro il db
		if !File.exists?(@db_file)
			#download db
			begin
				buck_db = Bucket.find(config["bucket_db"])
			  db_file = S3Object.find('s3rbackup_yaml_db', config["bucket_db"])
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

	def crea_bucket()
		#find bucket
		@bucket = @config["bucket"]
		@bucket_log = @config["bucket_log"] ? @config["bucket_log"] : "#{@bucket}-logs"
		begin
			bbackup = Bucket.find(@bucket)
		rescue
			if !Bucket.create(@bucket)
				raise "Can't create bucket:#{@bucket}"
			end
		end
		if @config["log"]
			begin
				blog = Bucket.find(@bucket_log)
			rescue
				Bucket.create(@bucket_log)
			end
			Bucket.enable_logging_for(
				@bucket, 'target_bucket' => @bucket_log)
		else
			Bucket.disable_logging_for(@bucket) 
		end
	end	

	def bak(dirs, name, descr)
		crea_bucket()
		name = dirs[0] if !name
		tf = Tempfile.new("s3rbackup")
		tf_l = Tempfile.new("s3rbackup-listfile")
		case @config["compression"]
			when '7z'
				tar = `tar -cv #{dirs.join(" ")}  2>#{tf_l.path} | 7z a -t7z -m0=lzma -mx=9 -mfb=64 -md=32m -ms=on -si  #{tf.path}.7z`
				file_path = "#{tf.path}.7z"
			when 'lzma'
				tar = `tar -cv #{dirs.join(" ")}  2>#{tf_l.path} | lzma -9 > #{tf.path}`
				file_path = tf.path
			when 'gz'
				tar = `tar -cv #{dirs.join(" ")}  2>#{tf_l.path} | gzip -9 > #{tf.path}`
				file_path = tf.path
			else
				tar = `tar -cv #{dirs.join(" ")}  2>#{tf_l.path} | bzip2 -9 > #{tf.path}`
				file_path = tf.path
		end
		#FIXME delete file_path

		filez = []
		File.open(tf_l.path, 'r').each_line do |fh|
    	filez << fh
		end

		doc = {}
		doc["name"] = name
		doc["bucket"] = @config["bucket"]
		doc["datetime"] = Time.now
		doc["current_path"] = `pwd`.gsub("\n","").to_s
		doc["description"] = descr
		doc["host"] = `hostname`.gsub("\n","").to_s
		doc["user"] = `whoami`.gsub("\n","").to_s
		doc["size"] = File.size(file_path)
		doc["compression"] = @config["compression"]
		doc["archive"] = "tar"
		doc["files"] = filez.join("")
		@db << doc
		aws_name = "#{doc["name"]}_#{`date +%Y%m%d_%H.%M.%S`}_#{@db.index(doc)}".gsub("\n","")
		doc["aws_name"] = aws_name
		#FIXME Controllare che in db venga salvato aws_name

  # Store it!
		options = {}
		#options[:access] = :public_read if @public
		options["x-amz-meta-host"] = doc["host"]
		options["x-amz-meta-user"] = doc["user"]
		options["x-amz-meta-descrizione"] = doc["description"]
		options["x-amz-meta-current_path"] = doc["current_path"]
		options["x-amz-meta-size"] = doc["size"]
		options["x-amz-meta-compression"] = doc["compression"]
		options["x-amz-meta-archive"] = doc["archive"]
		#options["x-amz-meta-files"] = doc["files"]


	#       options["x-amz-meta-sha1_hash"] = `sha1sum #{file}`.split[0] if @save_hash
	#         options["x-amz-meta-mtime"] = fstat.mtime.getutc.to_i if @save_time
	#           options["x-amz-meta-size"] = fstat.size if @save_size

		store = S3Object.store(aws_name, open(file_path), @config["bucket"], options)
		obj = S3Object.find(aws_name, @config["bucket"])
		#obj.store
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
			case word 
				when /.*=.*/
					#opzione
					option["="] ||= {}
					option["="][word.split("=")[0]] = word.split("=")[1]
				when /.*>.*/
					#opzione
					option[">"] ||= {}
					option[">"][word.split(">")[0]] = word.split(">")[1]
				when /.*<.*/
					#opzione
					option["<"] ||= {}
					option["<"][word.split("<")[0]] = word.split("<")[1]
				else
					words_search << word
			end
		end
		results = []
		@db.each do |item|
			option.each do |key,opts|
				opts.each do |campo,val|
					case key
						when "="
							case item[campo].class.to_s
								when "Time"
									results << item if item[campo] = Time.parse(val) 
								when "Fixnum"
									results << item if item[campo] = val.to_i
								else
									results << item if item[campo] =~ /.*#{val}.*/
							end
						when "<"
							case item[campo].class.to_s
								when "Time"
									results << item if item[campo] < Time.parse(val) 
								when "Fixnum"
									results << item if item[campo] < val.to_i
								else
									results << item if item[campo] < val
							end
						when ">"
							case item[campo].class.to_s
								when "Time"
									results << item if item[campo] > Time.parse(val) 
								when "Fixnum"
									results << item if item[campo] > val.to_i
								else
									results << item if item[campo] > val
							end
					end
				end
			end
			words_search.each do |word|
				if item.values.join(" ") =~ /.*#{word}.*/
					results << item
				end
			end
			#results.sort!
			and_results = []
			if results.nitems > 0
				prev = results[0]
				results.each do |res|
					test = results.select {|t| t == res}
					#test = results.select {|t| t["aws_name"] == res["aws_name"]}
					if words.nitems == test.nitems
						and_results << res
					end
				end
			end
			results = and_results
		end
		#caso in cui voglio tutto
		if words.nitems == 0
			@db.each do |item|
				results << item
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
		results.sort! {|x,y| x["datetime"] <=> y["datetime"]}

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
		tf = Tempfile.new("s3runbackup.#{item['archive']}.#{item['compression']}")
		open(tf.path, 'w') do |file|
			S3Object.stream(aws_name, bucket) do |chunk|
				file.write chunk
			end
		end
		if out_name
			`mkdir -p #{out_name}`
			out_tar = "--directory #{out_name}"
		else
			out_tar = ""
		end
		case item["compression"]
			when '7z'
				tar = `7za x -so #{tf.path} | tar #{out_tar} -xf -`
			when 'lzma'
				tar = `cat #{tf.path} | lzma -d -c | tar #{out_tar} xf -`
			when 'gz'
				tar = `tar #{out_tar} -xzf #{tf.path}`
			when 'bz2'
				tar = `tar #{out_tar} -xjf #{tf.path}`
			end
	end

	def delete(item)
		S3Object.delete item["aws_name"], item["bucket"]
		@db.delete(item)
	end

	def log()
		return Bucket.logs(@config["bucket_log"])
	end
end

class Configure
	attr_reader :current
	def initialize(file_name = "#{ENV['HOME']}/.s3rbackup/config.yml", config_num = nil)
		config_num = 0 if config_num == nil
		file_name = "#{ENV['HOME']}/.s3rbackup/config.yml" if !file_name
		@current = YAML::load(File.open(file_name))
		if @current.class.to_s == "Array"
			@current = @current[config_num]
		end
		if !@current["compression"] 
			@current["compression"] = "bz2"
		end
	end
end
