import re, math
from collections import Counter
import csv
import cosine_distance_objects

WORD = re.compile(r'\w+')

def get_cosine(vec1, vec2):
     intersection = set(vec1.keys()) & set(vec2.keys())
     numerator = sum([vec1[x] * vec2[x] for x in intersection])
     sum1 = sum([vec1[x]**2 for x in vec1.keys()])
     sum2 = sum([vec2[x]**2 for x in vec2.keys()])
     denominator = math.sqrt(sum1) * math.sqrt(sum2)

     if not denominator:
        return 0.0
     else:
        return float(numerator) / denominator

def text_to_vector(text):
     words = WORD.findall(text)
     return Counter(words)


def read_text():

    with open('/Users/nischalhp/Downloads/softwares/datasets/products_comparison/Redmart.csv','rU') as file_redmart:
        redmart_csv = csv.reader(file_redmart,delimiter=',')
        redmart_products = []
        next(redmart_csv,None)
        for row in redmart_csv:
            cosine_obj = cosine_distance_objects()
            cosine_obj.set_product_details(row[1].lower().strip(),row[2])
            redmart_products.append(cosine_obj)

    with open('/Users/nischalhp/Downloads/softwares/datasets/products_comparison/FairPrice.csv','rU') as file_fairprice:
        fairprice_csv = csv.reader(file_fairprice,delimiter=',')
        fairprice_products = []
        next(fairprice_csv,None)
        for row in fairprice_csv:
            cosine_obj = cosine_distance_objects()
            cosine_obj.set_product_details(row[1].lower().strip(),row[2])
            fairprice_products.append(cosine_obj)


    with open('/Users/nischalhp/Downloads/softwares/datasets/products_comparison/ColdStorage.csv','rU') as file_coldstorage:
        coldstorage_csv = csv.reader(file_coldstorage,delimiter=',')
        coldstorage_products = []
        next(coldstorage_csv,None)
        for row in coldstorage_csv:
            cosine_obj = cosine_distance_objects()
            cosine_obj.set_product_details(row[1].lower().strip(),row[2])
            coldstorage_products.append(cosine_obj)


    with open('/Users/nischalhp/Downloads/softwares/datasets/products_comparison/rm_fp.csv','w') as rm_fp_file:
        for product in redmart_products:
            for product_fp in fairprice_products:
                product_vector = text_to_vector(list(set(product.get_product_name())))
                product_fp_vector = text_to_vector(list(set(product_fp.get_product_name())))
                cosine_similarity = get_cosine(product_vector,product_fp_vector)
                # just to get the ones with maximum similarity
                if cosine_similarity > 0.79:
                    print 'similarity between %s and %s' %(product_vector,product_fp_vector)
                    rm_fp_file.write('%s,%s,%s \n'%(product.get_product_id(),product_fp.get_product_id(),cosine_similarity))

    with open('/Users/nischalhp/Downloads/softwares/datasets/products_comparison/rm_cs.csv','w') as rm_cs_file:
        for product in redmart_products:
            for product_cs in coldstorage_products:
                product_vector = text_to_vector(list(set(product.get_product_name())))
                product_cs_vector = text_to_vector(list(set(product_cs.get_product_name())))
                cosine_similarity = get_cosine(product_vector,product_cs_vector)
                if cosine_similarity > 0.79:
                    print 'similarity between %s and %s' %(product_vector,product_cs_vector)
                    rm_cs_file.write('%s,%s,%s \n'%(product.get_product_id(),product_cs.get_product_id(),cosine_similarity))


read_text()









