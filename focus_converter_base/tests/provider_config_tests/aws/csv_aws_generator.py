import csv
from faker import Faker
import random
from datetime import datetime, timedelta

class CSVAWSGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate_time_interval(self):
        start_date = self.fake.date_time_this_year()
        end_date = start_date + timedelta(days=random.randint(1, 30))
        return f"{start_date.isoformat()}/{end_date.isoformat()}"

    def generate_line_item(self):
        return {
            # Identity
            "identity/LineItemId": self.fake.uuid4(),
            "identity/TimeInterval": self.generate_time_interval(),

            # Bill
            "bill/InvoiceId": self.fake.uuid4(),
            "bill/InvoicingEntity": self.fake.company(),
            "bill/BillingEntity": self.fake.company_suffix(),
            "bill/BillType": random.choice(["Anniversary", "Monthly", "One-time"]),
            "bill/PayerAccountId": self.fake.uuid4(),
            "bill/BillingPeriodStartDate": self.fake.iso8601(),
            "bill/BillingPeriodEndDate": self.fake.iso8601(),

            # LineItem
            "lineItem/UsageAccountId": self.fake.uuid4(),
            "lineItem/LineItemType": random.choice(["Tax", "Discount", "Usage"]),
            "lineItem/UsageStartDate": self.fake.iso8601(),
            "lineItem/UsageEndDate": self.fake.iso8601(),
            "lineItem/ProductCode": self.fake.word(),
            "lineItem/UsageType": self.fake.word(),
            "lineItem/Operation": self.fake.word(),
            "lineItem/AvailabilityZone": self.fake.word(),
            "lineItem/UsageAmount": random.uniform(0, 100),
            "lineItem/NormalizationFactor": random.uniform(0, 2),
            "lineItem/NormalizedUsageAmount": random.uniform(0, 100),
            "lineItem/CurrencyCode": random.choice(["USD", "CAD", "EUR"]),
            "lineItem/UnblendedRate": random.uniform(0, 10),
            "lineItem/UnblendedCost": random.uniform(0, 1000),
            "lineItem/BlendedRate": random.uniform(0, 10),
            "lineItem/BlendedCost": random.uniform(0, 1000),
            "lineItem/LineItemDescription": self.fake.sentence(),
            "lineItem/TaxType": random.choice(["Canada Tax", "US Tax", "EU Tax"]),
            "lineItem/LegalEntity": self.fake.company(),

            # Product
            "product/ProductName": self.fake.word(),
            "product/availability": random.choice(["Available", "Unavailable"]),
            "product/durability": random.choice(["High", "Medium", "Low"]),
            "product/eventType": self.fake.word(),
            "product/feeCode": self.fake.word(),
            "product/feeDescription": self.fake.sentence(),
            "product/fromLocation": self.fake.city(),
            "product/fromLocationType": random.choice(["Urban", "Rural"]),
            "product/fromRegionCode": self.fake.country_code(),
            "product/group": self.fake.word(),
            "product/groupDescription": self.fake.sentence(),
            "product/location": self.fake.city(),
            "product/locationType": random.choice(["Urban", "Rural"]),
            "product/messageDeliveryFrequency": random.choice(["High", "Medium", "Low"]),
            "product/messageDeliveryOrder": random.choice(["Sequential", "Random"]),
            "product/operation": self.fake.word(),
            "product/overhead": self.fake.word(),
            "product/productFamily": self.fake.word(),
            "product/productType": self.fake.word(),
            "product/queueType": random.choice(["FIFO", "LIFO"]),
            "product/region": self.fake.city(),
            "product/regionCode": self.fake.country_code(),
            "product/servicecode": self.fake.word(),
            "product/servicename": self.fake.word(),
            "product/sku": self.fake.uuid4(),
            "product/storageClass": self.fake.word(),
            "product/storageMedia": random.choice(["SSD", "HDD"]),
            "product/toLocation": self.fake.city(),
            "product/toLocationType": random.choice(["Urban", "Rural"]),
            "product/toRegionCode": self.fake.country_code(),
            "product/transferType": random.choice(["Internal", "External"]),
            "product/usagetype": self.fake.word(),
            "product/version": self.fake.word(),
            "product/volumeType": random.choice(["EBS", "S3"]),

            # Pricing
            "pricing/RateCode": self.fake.word(),
            "pricing/RateId": self.fake.uuid4(),
            "pricing/currency": random.choice(["USD", "CAD", "EUR"]),
            "pricing/publicOnDemandCost": random.uniform(0, 1000),
            "pricing/publicOnDemandRate": random.uniform(0, 10),
            "pricing/term": random.choice(["Monthly", "Yearly"]),
            "pricing/unit": random.choice(["GB", "Hours"]),

            # Reservation
            "reservation/AmortizedUpfrontCostForUsage": random.uniform(0, 1000),
            "reservation/AmortizedUpfrontFeeForBillingPeriod": random.uniform(0, 1000),
            "reservation/EffectiveCost": random.uniform(0, 1000),
            "reservation/EndTime": self.fake.iso8601(),
            "reservation/ModificationStatus": random.choice(["Modified", "Unmodified"]),
            "reservation/NormalizedUnitsPerReservation": random.uniform(0, 100),
            "reservation/NumberOfReservations": random.randint(1, 10),
            "reservation/RecurringFeeForUsage": random.uniform(0, 1000),
            "reservation/StartTime": self.fake.iso8601(),
            "reservation/SubscriptionId": self.fake.uuid4(),
            "reservation/TotalReservedNormalizedUnits": random.uniform(0, 100),
            "reservation/TotalReservedUnits": random.uniform(0, 100),
            "reservation/UnitsPerReservation": random.uniform(0, 100),
            "reservation/UnusedAmortizedUpfrontFeeForBillingPeriod": random.uniform(0, 1000),
            "reservation/UnusedNormalizedUnitQuantity": random.uniform(0, 100),
            "reservation/UnusedQuantity": random.uniform(0, 100),
            "reservation/UnusedRecurringFee": random.uniform(0, 1000),
            "reservation/UpfrontValue": random.uniform(0, 1000),

            # SavingsPlan
            "savingsPlan/TotalCommitmentToDate": random.uniform(0, 10000),
            "savingsPlan/SavingsPlanARN": self.fake.uuid4(),
            "savingsPlan/SavingsPlanRate": random.uniform(0, 10),
            "savingsPlan/UsedCommitment": random.uniform(0, 10000),
            "savingsPlan/SavingsPlanEffectiveCost": random.uniform(0, 1000),
            "savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod": random.uniform(0, 1000),
            "savingsPlan/RecurringCommitmentForBillingPeriod": random.uniform(0, 1000),
        }

    def generate_and_write_csv(self, num_rows=10, filename='test_data.csv'):
        data = self.generate_data(num_rows)
        self.write_to_csv(data, filename)
        return filename
    
    def generate_data(self, num_rows=10):
        return [self.generate_line_item() for _ in range(num_rows)]
    
    def write_to_csv(self, data, filename='test_data.csv'):
        if data:
            keys = data[0].keys()
            with open(filename, 'w', newline='', encoding='utf-8') as file:
                dict_writer = csv.DictWriter(file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            print(f"Data written to {filename}")
        else:
            print("No data to write.")