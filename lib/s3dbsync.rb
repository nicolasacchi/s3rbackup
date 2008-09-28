require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'yaml'
#nuovo aws
require 'right_aws'
require 'net/smtp'

class S3SyncDb
	include AWS::S3
	attr :db
	def initialize(config)
		@conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => config["access_key_id"],
			    			:secret_access_key => config["secret_access_key"])
		@sdb = RightAws::SdbInterface.new(config["access_key_id"], config["secret_access_key"], 
															{:multi_thread => false, :logger => Logger.new('/tmp/sdb.log')})
		@s3 =  RightAws::S3.new(config["access_key_id"], config["secret_access_key"], 
															{:multi_thread => false, :logger => Logger.new('/tmp/s3.log')})

		@config = config
		#d'ora in poi uso il db su aws
		@domain_db = config['bucket']
		@bucket = config['bucket']
	end

	def initialize_db
		puts "Creating #{@domain_db} db..."
		@sdb.create_domain(@domain_db)
		if @config['files_db']
			puts "Creating file db #{@config['files_db']} db..."
			@sdb.create_domain(@config['files_db'])
		end
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
		if @config['files_db']
			puts "Deleting file db #{@config['files_db']} db..."
			@sdb.delete_domain(@config['files_db'])
		end
	end

	def test
		#buk = @s3.buckets
		#buk = @s3.buckets.map{|b| b.name}
 		#puts "Buckets on S3: #{buk.join(', ')}"
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
		doc["datetime"] = "'#{DateTime.now}'"
		#doc["date"] = "#{DateTime.now.strftime("%Y%m%d")}"
		doc["current_path"] = `pwd`.gsub("\n","").to_s
		doc["description"] = descr ? descr : "n/a"
		doc["host"] = `hostname`.gsub("\n","").to_s
		doc["user"] = `whoami`.gsub("\n","").to_s
		doc["size"] = File.size(file_path)
		doc["compression"] = @config["compression"]
		doc["archive"] = "tar"
		#@db << doc
		aws_name = "#{doc["name"]}##{`date +%Y%m%d_%H.%M.%S`}".gsub("\n","")
		doc["aws_name"] = aws_name
		@sdb.put_attributes @domain_db, aws_name, doc
		if @config['files_db']
			#tutti in uno
			filez_name = []
			filez.each do |fil|
				begin
					filez_name << File.basename(fil.gsub("\n", ""))
				rescue
				end
			end
			@sdb.put_attributes @config['files_db'], aws_name, { 
																:files_full => filez.map {|t| t.gsub("\n","")}, 
																:files_name => filez_name}
		end

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
		#obj = S3Object.find(aws_name, @config["bucket"])
		#obj.store
		#obj.about.each do |key,val|
		#	doc[key] = val
		#end
		#TODO aggiungere check
		send_mail("S3rbackup - Saved #{doc["name"]}", (doc.to_a.map {|val| "#{val[0]}: #{val[1]}"}).join("\n") + "\n\nFiles:\n\t#{filez.join("\t")}")
	end

	def find(words, bucket = nil, cmd_opt = {})
		option = {}
		words_search = []
		date = 0
		words.each do |word|
			date -= 1
			case word 
				when /.*=.*/
					#opzione
					words_search << word
				when /.*>.*/
					#opzione
					words_search << word
				when /.*<.*/
					#opzione
					words_search << word
				when /.*starts-with.*/
					words_search << word
				when /.*union.*/
					words_search << "] #{word} ["
				when /.*INTERSECTION.*/
					words_search << "] #{word} ["
				else
					if word == "datetime"
						date = 2
					end
					if date == 0
						#ci siamo uso i doppi apici
						words_search << "'\\'#{word}\\''"
					else
						words_search << "'#{word}'"
					end
			end
		end
		if words_search.nitems == 1
			#ho solo una parola uso startwith sul nome
			if cmd_opt[:inside]
				#cerco dentro ai file
				search = "['files_full' starts-with #{words_search[0]}] union ['files_name' starts-with #{words_search[0]}]"
			else
				search = "['aws_name' starts-with #{words_search[0]}] union ['description' starts-with #{words_search[0]}]"
			end
		elsif words_search.nitems == 0
			#devo caricare tutto
			search = "['datetime' > '\\'1970\\'']"
		else
			search = "[#{words_search.join(" ")}]"
		end
		results = []
		@sdb.query(cmd_opt[:inside] ? @config['files_db'] : @domain_db, search) do |result|
			result[:items].each do |item|
				hattr = @sdb.get_attributes(@domain_db, item)[:attributes]
				hattr_ok = {}
				hattr.each do |key,val|
					hattr_ok[key] = val[0]
					if cmd_opt[:files]
						#per ogni file scrico la lista
						hattr_ok_files ||= {}
						hattr_ok_files[key] = @sdb.get_attributes(@config['files_db'], item)[:attributes]
						p hattr_ok_files[key]	#FIXME vedere come visualizzarli...
					end
				end
				results << hattr_ok
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
		@sdb.delete_attributes(@domain_db, item["aws_name"])
	end

	def send_mail(subj, msg)
		if(@config['mail_to'] and @config['mail_to'] != "")
			msg = [ "Subject: #{subj}\n", "\n", "#{msg}\n" ]
			Net::SMTP.start('localhost') do |smtp|
				mail_to = @config['mail_to'].split(",").map {|t| t.strip} 
			  ret = smtp.sendmail( msg,  @config['mail_from'], mail_to)
			  #ret = smtp.sendmail( msg,  "nik@nikpad.ath.cx", ['sacchi.nicola@gmail.com'] )
			end
		end
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
