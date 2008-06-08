require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'active_record'
#FIXME creare un db in locale con le informazioni prese dai metadata

conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => 'xxxxxxxxxxxxxxxxxxx',
			    			:secret_access_key => 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

class S3SyncDb
	attr :db
	def initialize(config)
		@db_file = config[:db_file]
		@db_file = "#{ENV[:HOME]}/.s3backup/s3db.yml" if !@db_file
		@db_file_ver = "#{@db_file}.ver"
		#apro il db
		if !File.exists?(@db_file)
			#download db
			begin
				buck_db = Bucket.find(config.bucket_db)
			rescue
				#devo crearlo
				Bucket.create(config.bucket_db)
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
		S3Object.store("s3rbackup_yaml_db", open(@db_file, "r"), config.bucket_db)
		db_file = S3Object.find('s3rbackup_yaml_db', config.bucket_db)
		db_file[:version] = @version.to_s
	end
end

#a = S3SyncDb.new
#p a
