from database import Database
from helper.writeAJson import writeAJson
from main import db, result


class ProductAnalyzer:
    def __init__(self,database, collection):
        self.db = Database(database,collection)


    def total_vendas_dia(self):
        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": {"data": "$data_compra", "compra_id": "$compra_id", "produto": "$produtos.nome"},"quantidade_total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"_id.data": 1, "_id.compra_id": 1, "quantidade_total": -1}},
            {"$group": {"_id": {"data": "$_id.data", "compra_id": "$_id.compra_id"}, "produto_mais_vendido": {"$first": {"produto": "$_id.produto", "quantidade_total": "$quantidade_total"}}}},
            {"$project": {
                "_id": 0,
                "data": "$_id.data",
                "compra_id": "$_id.compra_id",
                "produto": "$produto_mais_vendido.produto",
                "quantidade_total": "$produto_mais_vendido.quantidade_total"
            }}
        ])

        writeAJson(result, "Total vendas dia")



    def produto_mais_vendido(self):
        result = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.nome","quantidade_total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"quantidade_total": -1}},
            {"$limit": 1},
            {"$project": {"_id": 0,"produto": "$_id","quantidade_total": 1}}
        ])

        writeAJson(result, "Produto mais vendido")

    def comprador_compra_mais_cara(self):
        result = db.collection.aggregate([
            {"$group": {"_id": "$cliente_id","total_gasto": {"$sum": "$total"}}},
            {"$sort": {"total_gasto": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "comprador_id": "$_id",
                "total_gasto": 0
            }}
        ])

        writeAJson(result, "Compra mais cara")

    def produtos_vendidos_acima(self):
        result = db.collection.aggregate([
            {"$group": { "_id": {"compra_id": "$compra_id", "produto": "$produtos.nome"}, "quantidade_total": {"$sum": "$produtos.quantidade"}}},
            {"$match": {"quantidade_total": {"$gt": 1}}},
            {"$group": {"_id": "$_id.produto","quantidade_total_vendida": {"$sum": "$quantidade_total"}}},
            {"$project": {
                "_id": 0,
                "produto": "$_id",
                "quantidade_total_vendida": 1
            }}
        ])

        writeAJson(result, "Produtos vendidos mais de uma unidade")

# Instanciando a classe procuctAnalyzer
analizer = ProductAnalyzer(database="mercado", collection="compras")

analizer.total_vendas_dia()
analizer.produto_mais_vendido()
analizer.comprador_compra_mais_cara()
analizer.produtos_vendidos_acima()
