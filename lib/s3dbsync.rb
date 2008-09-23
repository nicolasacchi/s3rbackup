require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'yaml'
#nuovo aws
require 'right_aws'

class S3SyncDb
	include AWS::S3
	attr :db
	def initialize(config)
		@conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => config["access_key_id"],
			    			:secret_access_key => config["secret_access_key"])
		@sdb = RightAws::SdbInterface.new(config["access_key_id"], config["secret_access_key"], 
															{:multi_thread => true, :logger => Logger.new('/tmp/x.log')})
		@s3 =  RightAws::S3.new(config["access_key_id"], config["secret_access_key"])

		@config = config
		#d'ora in poi uso il db su aws
		@domain_db = config['bucket']
		@bucket = config['bucket']
	end

	def initialize_db
		puts "Creating #{@domain_db} db..."
		@sdb.create_domain(@domain_db) 
		puts "Creating #{@bucket} bucket..."
		begin
			bbackup = Bucket.find(@bucket)
		rescue
			if !Bucket.create(@bucket)
				raise "Can't create bucket:#{@bucket}"
			end
		end
	end

	def destroy_db
		puts "Deleting bucket #{@bucket}..."
		bucket1 = @s3.bucket(@bucket)
		bucket1.delete(true)
		puts "Deleting db #{@domain_db}..."
		@sdb.delete_domain(@domain_db)
	end

	def test
		#buk = @s3.buckets
		buk = @s3.buckets.map{|b| b.name}
 		puts "Buckets on S3: #{buk.join(', ')}"
	end

	#s3 sdb
	def bak(dirs, name, descr)
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
		#doc["files"] = filez.join("")
		#@db << doc
		aws_name = "#{doc["name"]}##{`date +%Y%m%d_%H.%M.%S`}".gsub("\n","")
		doc["aws_name"] = aws_name
		@sdb.put_attributes @domain_db, aws_name, doc
		#TODO aggiungere md5

  # Store it!
		options = {}
		options["x-amz-meta-host"] = doc["host"]
		options["x-amz-meta-user"] = doc["user"]
		options["x-amz-meta-descrizione"] = doc["description"]
		options["x-amz-meta-current_path"] = doc["current_path"]
		options["x-amz-meta-size"] = doc["size"]
		options["x-amz-meta-compression"] = doc["compression"]
		options["x-amz-meta-archive"] = doc["archive"]

		store = S3Object.store(aws_name, open(file_path), @config["bucket"], options)
		obj = S3Object.find(aws_name, @config["bucket"])
		#obj.store
		obj.about.each do |key,val|
			doc[key] = val
		end
		#TODO aggiungere check
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
					words_search << word
				when /.*>.*/
					#opzione
					option[">"] ||= {}
					option[">"][word.split(">")[0]] = word.split(">")[1]
					words_search << word
				when /.*<.*/
					#opzione
					option["<"] ||= {}
					option["<"][word.split("<")[0]] = word.split("<")[1]
					words_search << word
				else
					words_search << "'#{word}'"
			end
		end
		if words_search.nitems == 1
			#ho solo una parola uso startwith sul nome
			search = "['aws_name' starts-with #{words_search[0]}]"
		else
			search = "[#{words_search.join(" ")}]"
		end
		results = []
		@sdb.query(@domain_db, search) do |result|
			result[:items].each do |item|
				results << @sdb.get_attributes(@domain_db, item)[:attributes]
			end
		end
		#results_aws[:items].each do |item|
		#end
		#p results
#		hres = {}
#		if cmd_opt[:newer] or cmd_opt[:older]
#			results.each do |item|
#				results.each do |item2|
#					if item["name"] == item2["name"]
#						hres[item["name"]] ||= []
#						hres[item["name"]] << item
#						#hres[:name].sort! {|x,y| x["date"] <=> y["date"]}
#					end
#				end
#			end
#			results = []
#			#p hres
#			hres.each do |key,val|
#				val.sort! {|x,y| x["date"] <=> y["date"]}
#				results << val[val.nitems - 1] if cmd_opt[:newer]
#				results << val[0] if cmd_opt[:older]
#			end
#		end
#		results.sort! {|x,y| x["datetime"] <=> y["datetime"]}

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
