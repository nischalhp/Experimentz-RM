class MemberProduct:

	averageItemsInBasket = 0
	frequencyOfProductsWeek = {}

	def setFreqMap(self,productFreqHash):
            self.frequencyOfProductsWeek = productFreqHash

	def getFrequencyOfProduct(self):
		return self.frequencyOfProductsWeek

	def modifyAverageItemsInBasket(self,avg):
		self.averageItemsInBasket = avg

	def getAverageBasketSize(self):
		return self.averageItemsInBasket


