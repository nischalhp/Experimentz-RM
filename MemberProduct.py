class MemberProduct:

	averageItemsInBasket = 0
	frequencyOfProducts = {}

	def add_or_modify_product_freq(self,productId):
		productIds = self.frequencyOfProducts.keys()
		# which means the user has already purchased this product before
		if productId in productIds:
			freqOfProduct = self.frequencyOfProducts[productId]
			freqOfProduct += 1
			self.frequencyOfProducts[productId] = freqOfProduct
		else:
			# first time buy
			self.frequencyOfProducts[productId] = 1

	def getFrequencyOfProduct(self):
		return self.frequencyOfProducts

	def modifyAverageItemsInBasket(self,avg):
		self.averageItemsInBasket = avg

	def getAverageBasketSize(self):
		return self.averageItemsInBasket


