require 'rubygems'
require 'aws/s3'
require 'tempfile'
require 'active_record'
require 's3dbsync'
require 'optparse'
require 'yaml'
#FIXME creare un db in locale con le informazioni prese dai metadata

class S3Backup
	def initialize(bucket)
		#find bucket
		@bucket = bucket
		begin
			bbackup = Bucket.find(bucket)
		rescue
			if !Bucket.create(bucket)
				raise "Can't find bucket:#{bucket}"
			end
		end
	end	

	def bak(dirs)
		tf = Tempfile.new("s3backup")
		tar = `tar -c #{dirs.join(" ")} | bzip2 -9 > #{tf.path}`
		
		doc = Doc.new
		doc.nome = dirs[0]  #FIXME aggiungere da riga di comando
		doc.current_path = `pwd`
		doc.descrizione = "testing"  #FIXME aggiungere da riga di comando
		doc.host = `hostname`
		doc.user = `whoami`
		doc.save
		aws_name = "#{dirs[0]}_#{`date +%Y%m%d`}_#{doc.id}"
		doc.aws_name = aws_name
		doc.save

		store = S3Object.store(aws_name, open(tf.path), @bucket)
		obj = S3Object.find(aws_name, @bucket)
		obj.metadata[:host] = doc.host
		obj.metadata[:user] = doc.user
		obj.metadata[:descrizione] = doc.descrizione
		obj.metadata[:current_path] = doc.current_path
		obj.store
		p obj.about
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

options = {}
OptionParser.new do |opts|
	opts.banner = "Usage: s3backup.rb [options] <files>"

	opts.on("-n", "--name [NAME]", String, "Backup name") do |name|
		options[:name] = name
	end

	opts.on("-b", "--bucket [BUCKET]", String, "Bucket name") do |bucket|
		options[:bucket] = bucket
	end

	opts.on("-d", "--description [DESCRIPTION]", String, "Description") do |name|
		options[:descr] = name
	end
	
	opts.on("-c", "--file-cfg [PATH]", String, "Path of cfg file") do |name|
		options[:file_cfg] = name
	end
	
	opts.on("-s", "--nosync-db", "Don't sync local db with remote") do |s|
		options[:nosync] = s
	end
end.parse!

p options
p ARGV

config = Configure.new(options[:file_cfg])

#s3db = S3SyncDb.new
#s3b = S3Backup.new(ARGV.shift)
#s3b.bak(ARGV)
#s3db.aggiornaS3
