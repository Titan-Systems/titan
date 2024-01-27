create pipe mypipe as copy into mytable from @mystage;

create pipe mypipe2 as copy into mytable(C1, C2) from (select $5, $4 from @mystage);

create pipe mypipe_s3
  auto_ingest = true
  aws_sns_topic = 'arn:aws:sns:us-west-2:001234567890:s3_mybucket'
  as
  copy into snowpipe_db.public.mytable
  from @snowpipe_db.public.mystage
  file_format = (type = 'JSON');

create pipe mypipe_gcs
  auto_ingest = true
  integration = 'MYINT'
  as
  copy into snowpipe_db.public.mytable
  from @snowpipe_db.public.mystage
  file_format = (type = 'JSON');

create pipe mypipe_azure
  auto_ingest = true
  integration = 'MYINT'
  as
  copy into snowpipe_db.public.mytable
  from @snowpipe_db.public.mystage
  file_format = (type = 'JSON');