Gem::Specification.new do |s|
  s.name = "s3rbackup"
  s.version = "0.1.0"
  s.date = "2008-06-13"
  s.summary = "Simple backup on Amazon S3"
  s.email = "sacchi.nicola+s3rbackup@gmail.com"
  s.homepage = "http://github.com/niky81/s3rbackup"
  s.description = "s3rbackup is a command line program for backing and restore group of directory or file in s3, it ships with integrated database for search in backup data."
  s.has_rdoc = false
  s.authors = ["Nicola Sacchi"]
  s.files = ["LICENSE", "README" , "s3dbsync.rb", "s3query.rb", "s3rbackup.rb"]
  s.test_files = []
  s.add_dependency("aws-s3", ["> 0.0.0"])
  s.add_dependency("OptionParser", ["> 0.0.0"])
end
