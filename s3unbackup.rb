require 'rubygems'
require 'aws/s3'
require 'tempfile'

include AWS::S3

conn = AWS::S3::Base.establish_connection!(
	    					:access_key_id     => 'xxxxxxxxxxxx',
			    			:secret_access_key => 'xxxxxxxxxxxxxxxxxxx')
#p conn
#p Service.buckets
bucket = ARGV.shift
obj = ARGV.shift

#find bucket
begin
	bbackup = Bucket.find(bucket)
	p bbackup
rescue
	p "#{bucket} doesent exist"
	return 1
end

tf = Tempfile.new("s3unbackup")

open(tf.path, 'w') do |file|
	S3Object.stream(obj, bucket) do |chunk|
		file.write chunk
	end
end
tar = `tar xfj #{tf.path}`

#TODO aggiungere check
