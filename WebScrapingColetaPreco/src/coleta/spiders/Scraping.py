from typing import Iterable
import scrapy
from coleta.items import ColetaItem
from urllib.parse import urlencode
from datetime import datetime
import pytz


API_KEY = '1e799199-1a88-45d5-813a-7c928c1d8155'
categorias_amazon = ['eletrodomesticos', 'pneus', 'notebook', 'impressora']
categorias_mercado_livre = ["impressora"]
#categorias_mercado_livre = ["impressora", "notebook", "pneus", "eletrodomesticos"]

def get_scrapeops_url(url):
    payload = {'api_key': API_KEY, 'url': url}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url

class AmazonSpider(scrapy.Spider):
    name = "amazon_spider"
    start_urls = ["https://www.amazon.com.br"]
    
    def start_requests(self):
        for categoria in categorias_amazon:
            for page in range(1):  # Altere o range para a quantidade de páginas que deseja
                url = f'https://www.amazon.com.br/s?k={categoria}&page={page}'
                yield scrapy.Request(get_scrapeops_url(url), callback=self.parse_amazon, meta={'categoria': categoria})

    def parse_amazon(self, response):
        categoria_atual = response.meta['categoria']
        # Seleciona os produtos na página
        products = response.css('div.s-main-slot div.s-result-item')

        for produto in products:
            try:
                item = ColetaItem()
                item['dsc_produto'] = produto.css('h2 a span::text').get()  
                # item['preco_old']   = produto.css('span.a-price.a-text-price[data-a-strike="true"] .a-offscreen::text').get()
                item['preco_new']   = produto.css('span.a-price-whole::text').get()  
                item['cent_new']    = produto.css('span.a-price-fraction::text').get()  
                item['link']        = 'https://www.amazon.com.br/' + produto.css('h2 a::attr(href)').get()  
                item['categoria']   = categoria_atual
                item['fonte']       = 'Amazon'
                if item['dsc_produto'] and item['preco_new']:
                    yield item
            except Exception as e:
                self.logger.error(f"Erro ao realizar o crawl do produto Amazon: {e}")


class MercadoLivreSpider(scrapy.Spider):
    name = "mercado_livre"
    page_count = 1
    max_pages = 1

    def start_requests(self):
        for categoria in categorias_mercado_livre:
            url = f"https://lista.mercadolivre.com.br/{categoria}"
            yield scrapy.Request(url=url, callback=self.parse_mercado_livre, meta={'categoria': categoria})

    def parse_mercado_livre(self, response):
        categoria_atual = response.meta['categoria']
        
        if categoria_atual in ['pneus', "eletrodomesticos"]:
            products = response.css('div.ui-search-result__content-wrapper')
        else:
            products = response.css('div.ui-search-result__content') #poly-card__content' #ui-search-result__content #poly-component__price

        for produto in products:
            try:
              

                 # Adicionar coluna com data da coleta
                fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
                hora_atual_brasil = datetime.now(fuso_horario_brasil)
                data_formatada = hora_atual_brasil.strftime("%Y-%m-%d %H:%M:%S")

                prices = produto.css('span.andes-money-amount__fraction::text').getall()
                cents = produto.css('span.andes-money-amount__cents::text').getall()
                item = ColetaItem()
                item['dsc_produto'] = produto.css('a.ui-search-link__title-card.ui-search-link::text').get()
                item['preco_new']   = str(prices[1]) if len(prices) > 1 else None
                item['cent_new']    = cents[1] if len(cents) > 1 else None
                item['link']        = produto.css('a::attr(href)').get()
                item['categoria']   = categoria_atual
                item['fonte']       = 'Mercado Livre'
                item['data_int']    = data_formatada

                if item['dsc_produto'] and item['preco_new']:
                    yield item
            except Exception as e:
                self.logger.error(f"Erro ao realizar o crawl do produto Mercado Livre: {e}")

        # Paginação
        if self.page_count < self.max_pages:
            next_page = response.css("li.andes-pagination__button.andes-pagination__button--next a::attr(href)").get()
            if next_page and self.page_count < self.max_pages:
                self.page_count += 1
                yield scrapy.Request(url=next_page, callback=self.parse_mercado_livre, meta={'categoria': categoria_atual})
