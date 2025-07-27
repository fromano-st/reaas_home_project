#!/usr/bin/env python3
"""
Unit tests for the e-commerce transaction producer
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from producer.ecommerce_producer import EcommerceDataGenerator, EcommerceProducer, EcommerceTransaction


class TestEcommerceDataGenerator(unittest.TestCase):
    """Test the data generator functionality"""
    
    def setUp(self):
        self.generator = EcommerceDataGenerator()
    
    def test_generate_transaction_structure(self):
        """Test that generated transactions have the correct structure"""
        transaction = self.generator.generate_transaction()
        
        # Check required fields exist
        self.assertIsNotNone(transaction.transaction_id)
        self.assertIsNotNone(transaction.customer_id)
        self.assertIsNotNone(transaction.product_id)
        self.assertIsNotNone(transaction.timestamp)
        
        # Check data types
        self.assertIsInstance(transaction.quantity, int)
        self.assertIsInstance(transaction.unit_price, float)
        self.assertIsInstance(transaction.total_amount, float)
        self.assertIsInstance(transaction.is_fraudulent, bool)
        self.assertIsInstance(transaction.event_duration, float)
    
    def test_transaction_id_format(self):
        """Test transaction ID format"""
        transaction = self.generator.generate_transaction()
        self.assertTrue(transaction.transaction_id.startswith('txn_'))
        self.assertEqual(len(transaction.transaction_id), 12)  # txn_ + 8 chars
    
    def test_customer_id_format(self):
        """Test customer ID format"""
        transaction = self.generator.generate_transaction()
        self.assertTrue(transaction.customer_id.startswith('cust_'))
    
    def test_positive_amounts(self):
        """Test that generated amounts are positive"""
        transaction = self.generator.generate_transaction()
        self.assertGreater(transaction.unit_price, 0)
        self.assertGreater(transaction.total_amount, 0)
        self.assertGreaterEqual(transaction.discount_amount, 0)
        self.assertGreaterEqual(transaction.tax_amount, 0)
    
    def test_quantity_range(self):
        """Test that quantity is within expected range"""
        transaction = self.generator.generate_transaction()
        self.assertGreaterEqual(transaction.quantity, 1)
        self.assertLessEqual(transaction.quantity, 5)
    
    def test_location_structure(self):
        """Test location data structure"""
        transaction = self.generator.generate_transaction()
        self.assertIsNotNone(transaction.location.city)
        self.assertIsNotNone(transaction.location.state)
        self.assertIsNotNone(transaction.location.country)
        self.assertIsNotNone(transaction.location.zip_code)
    
    def test_payment_method_validity(self):
        """Test that payment method is from valid list"""
        valid_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
        transaction = self.generator.generate_transaction()
        self.assertIn(transaction.payment_method, valid_methods)
    
    def test_device_type_validity(self):
        """Test that device type is from valid list"""
        valid_devices = ["mobile", "desktop", "tablet"]
        transaction = self.generator.generate_transaction()
        self.assertIn(transaction.device_type, valid_devices)
    
    def test_fraud_rate(self):
        """Test that fraud rate is approximately 2%"""
        transactions = [self.generator.generate_transaction() for _ in range(1000)]
        fraud_count = sum(1 for t in transactions if t.is_fraudulent)
        fraud_rate = fraud_count / len(transactions)
        
        # Allow some variance (1% to 4%)
        self.assertGreaterEqual(fraud_rate, 0.01)
        self.assertLessEqual(fraud_rate, 0.04)
    
    def test_event_duration_range(self):
        """Test event duration is within expected range"""
        transaction = self.generator.generate_transaction()
        self.assertGreaterEqual(transaction.event_duration, 30.0)
        self.assertLessEqual(transaction.event_duration, 600.0)


class TestEcommerceProducer(unittest.TestCase):
    """Test the Kafka producer functionality"""
    
    def setUp(self):
        self.mock_kafka_producer = Mock()
        
    @patch('producer.ecommerce_producer.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_class):
        """Test producer initialization"""
        mock_kafka_class.return_value = self.mock_kafka_producer
        
        producer = EcommerceProducer(
            bootstrap_servers='localhost:9092',
            topic='test-topic'
        )
        
        self.assertEqual(producer.bootstrap_servers, 'localhost:9092')
        self.assertEqual(producer.topic, 'test-topic')
        mock_kafka_class.assert_called_once()
    
    @patch('producer.ecommerce_producer.KafkaProducer')
    def test_send_transaction_success(self, mock_kafka_class):
        """Test successful transaction sending"""
        # Setup mocks
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = 'test-topic'
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 123
        mock_future.get.return_value = mock_record_metadata
        
        self.mock_kafka_producer.send.return_value = mock_future
        mock_kafka_class.return_value = self.mock_kafka_producer
        
        # Create producer and transaction
        producer = EcommerceProducer(topic='test-topic')
        generator = EcommerceDataGenerator()
        transaction = generator.generate_transaction()
        
        # Test sending
        result = producer.send_transaction(transaction)
        
        self.assertTrue(result)
        self.mock_kafka_producer.send.assert_called_once()
        mock_future.get.assert_called_once()
    
    @patch('producer.ecommerce_producer.KafkaProducer')
    def test_send_transaction_failure(self, mock_kafka_class):
        """Test transaction sending failure"""
        # Setup mocks
        mock_future = Mock()
        mock_future.get.side_effect = Exception("Kafka error")
        
        self.mock_kafka_producer.send.return_value = mock_future
        mock_kafka_class.return_value = self.mock_kafka_producer
        
        # Create producer and transaction
        producer = EcommerceProducer(topic='test-topic')
        generator = EcommerceDataGenerator()
        transaction = generator.generate_transaction()
        
        # Test sending
        result = producer.send_transaction(transaction)
        
        self.assertFalse(result)
    
    @patch('producer.ecommerce_producer.KafkaProducer')
    def test_transaction_serialization(self, mock_kafka_class):
        """Test that transactions are properly serialized"""
        mock_kafka_class.return_value = self.mock_kafka_producer
        
        producer = EcommerceProducer(topic='test-topic')
        generator = EcommerceDataGenerator()
        transaction = generator.generate_transaction()
        
        # Mock successful send
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_future.get.return_value = mock_record_metadata
        self.mock_kafka_producer.send.return_value = mock_future
        
        producer.send_transaction(transaction)
        
        # Check that send was called with correct parameters
        call_args = self.mock_kafka_producer.send.call_args
        self.assertEqual(call_args[1]['topic'], 'test-topic')
        self.assertEqual(call_args[1]['key'], transaction.customer_id)
        
        # Check that value is a dictionary (serializable)
        value = call_args[1]['value']
        self.assertIsInstance(value, dict)
        self.assertEqual(value['transaction_id'], transaction.transaction_id)


class TestEcommerceTransaction(unittest.TestCase):
    """Test the transaction data class"""
    
    def test_transaction_creation(self):
        """Test transaction object creation"""
        from producer.ecommerce_producer import Location
        
        location = Location(
            city="Seattle",
            state="WA", 
            country="USA",
            zip_code="98101"
        )
        
        transaction = EcommerceTransaction(
            transaction_id="txn_12345",
            customer_id="cust_6789",
            product_id="prod_2468",
            product_name="Test Product",
            category="Electronics",
            quantity=2,
            unit_price=29.99,
            total_amount=59.98,
            discount_amount=5.00,
            tax_amount=4.80,
            timestamp="2023-06-15T14:22:35Z",
            payment_method="credit_card",
            location=location,
            is_fraudulent=False,
            session_id="sess_abc123",
            device_type="mobile",
            user_agent="Mozilla/5.0...",
            ip_address="192.168.1.1",
            event_duration=120.5
        )
        
        self.assertEqual(transaction.transaction_id, "txn_12345")
        self.assertEqual(transaction.customer_id, "cust_6789")
        self.assertEqual(transaction.location.city, "Seattle")
        self.assertFalse(transaction.is_fraudulent)


if __name__ == '__main__':
    unittest.main()