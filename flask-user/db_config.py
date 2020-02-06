from app import app
from flaskext.mysql import MySQL

mysql = MySQL()
 
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = '**********'
app.config['MYSQL_DATABASE_PASSWORD'] = '**********'
app.config['MYSQL_DATABASE_DB'] = '<database_name>'
app.config['MYSQL_DATABASE_HOST'] = '<host_name>'
mysql.init_app(app)
