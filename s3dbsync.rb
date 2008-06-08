require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'active_record'
#FIXME creare un db in locale con le informazioni prese dai metadata

# define a migration
class CreateTasks < ActiveRecord::Migration
  def self.up
    create_table :docs do |t|
	    t.string :nome
			t.string :aws_name
			t.string :descrizione
			t.string :current_path
			t.string :host
			t.string :user
			t.datetime :created_on
			t.datetime :updated_on
    end
    create_table :items do |t|
	    t.string :path
			t.string :doc_id
    end
  end

  def self.down
    drop_table :docs
    drop_table :files
  end
end
class Doc < ActiveRecord::Base
end
class Item < ActiveRecord::Base
end

include AWS::S3

conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => 'xxxxxxxxxxxxxxxxxxx',
			    			:secret_access_key => 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

class S3SyncDb
	def initialize
		@db_file = "/home/nik/.s3backup/s3db.sqlite"
		@db_file_ver = "#{@db_file}.ver"
		@db_bucket = "nik_db_bucket"
		if !File.exists?(@db_file)
			`mkdir -p #{File.dirname(@db_file)}`
			`echo 0 > #{@db_file_ver}`
		end
		# connect to the database (sqlite in this case)
		ActiveRecord::Base.establish_connection({
	  		:adapter => "sqlite3", 
	  		:dbfile => @db_file 
		})
		CreateTasks.migrate(:up)
		#controllo che su S3 esista il tutto
#		begin
#			buck_db = Bucket.find(@db_bucket)
#		rescue
#			if Bucket.create(@db_bucket)
#				buck_db = Bucket.find(@db_bucket)
#			else
#				raise "Can't create bucket"
#			end
#		end
#		begin
#		  versione = S3Object.find('version', @db_bucket)
#		rescue
#			S3Object.store('version', "0", @db_bucket)
#		  versione = S3Object.find('version', @db_bucket)
#		end
#
#		#controllo versione
#		File.open(@db_file_ver, 'r').each do |line2|
#			if versione.value.to_i > line2.to_i
#					p "devo aggiornare"
#			else
#					p "sono a posto"
#			end
#			@cur_ver = versione.value.to_i
#		end
	end

	def aggiornaS3
		S3Object.store("version", (@cur_ver + 1).to_s, @db_bucket)
		file = File.open(@db_file_ver, "w")
		file.write((@cur_ver + 1).to_s)
		file.close
	end
end

#a = S3SyncDb.new
#p a
