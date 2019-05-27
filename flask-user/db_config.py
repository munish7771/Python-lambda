from app import app
from flaskext.mysql import MySQL

mysql = MySQL()
 
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'hudZrh49l0'
app.config['MYSQL_DATABASE_PASSWORD'] = 'QQdwj3GfQH'
app.config['MYSQL_DATABASE_DB'] = 'hudZrh49l0'
app.config['MYSQL_DATABASE_HOST'] = 'remotemysql.com'
mysql.init_app(app)