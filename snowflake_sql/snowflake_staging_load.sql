CREATE FILE FORMAT IF NOT EXISTS csv_format
  TYPE = 'CSV' 
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


CREATE OR REPLACE STAGE snowstage
FILE_FORMAT = csv_format
URL='s3://s3airbnbprojectbdv/source/';
    

COPY INTO LISTINGS
FRoM @snowstage
FILES=('listings.csv')
CREDENTIALS=(aws_key_id = 'yourkey', aws_secret_key = 'yoursecretkey');

COPY INTO BOOKINGS
FRoM @snowstage
FILES=('bookings.csv')
CREDENTIALS=(aws_key_id = 'yourkey', aws_secret_key = 'yoursecretkey');

COPY INTO HOSTS
FRoM @snowstage
FILES=('hosts.csv')
CREDENTIALS=(aws_key_id = 'yourkey', aws_secret_key = 'yoursecretkey');
