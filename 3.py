from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
#........... ваши датафреймы
#...........ваши колонки
product_category_pairs = products.join(categories, 'product_id')#Соединение датафреймов
product_category_pairs = product_category_pairs.select('product_name', 'category_name')# Выбираем 'product_name' и 'category_name'
product_category_pairs.show()

###################################
products_without_categories = products.join(categories, 'product_id', 'left_anti')# left join и фильтр
products_without_categories = products_without_categories.select('product_name')#Выбираем product_name
products_without_categories.show()
