"""
Unit tests for RewardSense schemas.

Tests Story 6.1 acceptance criteria:
- All data artifacts have defined schemas
- Schemas are version controlled
- Schema validation works correctly
"""

import pytest
from pydantic import ValidationError

from schemas import (
    TransactionRaw,
    TransactionCleaned,
    UserProfileRaw,
    FeatureMetadata,
    FeatureRegistry,
    validate_user_id_format,
    validate_transaction_id_format,
    validate_category,
    validate_mcc_code,
)


class TestTransactionSchemas:
    """Test transaction schemas."""

    def test_transaction_raw_valid(self):
        """Test valid transaction passes validation."""
        data = {
            "transaction_id": "txn_0000123",
            "user_id": "user_0001",
            "date": "2025-08-01",
            "category": "dining",
            "merchant": "Starbucks",
            "mcc_code": 5812,
            "amount": 16.09,
            "card_used": "Chase Sapphire Reserve",
        }

        txn = TransactionRaw(**data)
        assert txn.transaction_id == "txn_0000123"
        assert txn.amount == 16.09

    def test_transaction_raw_invalid_user_id(self):
        """Test invalid user_id format raises error."""
        data = {
            "transaction_id": "txn_0000123",
            "user_id": "invalid_123",  # Wrong format
            "date": "2025-08-01",
            "category": "dining",
            "merchant": "Starbucks",
            "mcc_code": 5812,
            "amount": 16.09,
            "card_used": "Card",
        }

        with pytest.raises(ValidationError):
            TransactionRaw(**data)

    def test_transaction_raw_negative_amount(self):
        """Test negative amount raises error."""
        data = {
            "transaction_id": "txn_0000123",
            "user_id": "user_0001",
            "date": "2025-08-01",
            "category": "dining",
            "merchant": "Starbucks",
            "mcc_code": 5812,
            "amount": -10.0,  # Negative
            "card_used": "Card",
        }

        with pytest.raises(ValidationError):
            TransactionRaw(**data)

    def test_transaction_cleaned_has_suspicious(self):
        """Test cleaned transaction includes suspicious flag."""
        data = {
            "transaction_id": "txn_0000123",
            "user_id": "user_0001",
            "date": "2025-08-01",
            "category": "dining",
            "merchant": "Starbucks",
            "mcc_code": 5812,
            "amount": 16.09,
            "card_used": "Card",
            "suspicious": False,
        }

        txn = TransactionCleaned(**data)
        assert hasattr(txn, "suspicious")
        assert txn.suspicious == False


class TestUserProfileSchemas:
    """Test user profile schemas."""

    def test_user_profile_raw_valid(self):
        """Test valid user profile passes validation."""
        data = {
            "user_id": "user_0001",
            "archetype": "high_roller",
            "monthly_budget": 13156.94,
            "cards": "['Chase Sapphire Reserve']",
            "redemption_preference": "travel_portal",
            "age_group": "51-65",
            "location_type": "urban",
        }

        user = UserProfileRaw(**data)
        assert user.archetype == "high_roller"
        assert user.monthly_budget > 0

    def test_user_profile_invalid_archetype(self):
        """Test invalid archetype raises error."""
        data = {
            "user_id": "user_0001",
            "archetype": "invalid_type",  # Invalid
            "monthly_budget": 1000,
            "cards": "['Card']",
            "redemption_preference": "cash_back",
            "age_group": "26-35",
            "location_type": "urban",
        }

        with pytest.raises(ValidationError):
            UserProfileRaw(**data)

    def test_user_profile_invalid_age_group(self):
        """Test invalid age group raises error."""
        data = {
            "user_id": "user_0001",
            "archetype": "high_roller",
            "monthly_budget": 1000,
            "cards": "['Card']",
            "redemption_preference": "cash_back",
            "age_group": "100-200",  # Invalid
            "location_type": "urban",
        }

        with pytest.raises(ValidationError):
            UserProfileRaw(**data)


class TestValidators:
    """Test validation utility functions."""

    def test_validate_user_id_valid(self):
        """Test valid user_id passes."""
        assert validate_user_id_format("user_0001") == "user_0001"
        assert validate_user_id_format("user_9999") == "user_9999"

    def test_validate_user_id_invalid(self):
        """Test invalid user_id raises error."""
        with pytest.raises(ValueError):
            validate_user_id_format("invalid_001")

        with pytest.raises(ValueError):
            validate_user_id_format("user_abc")

    def test_validate_transaction_id_valid(self):
        """Test valid transaction_id passes."""
        assert validate_transaction_id_format("txn_0000123") == "txn_0000123"

    def test_validate_category_valid(self):
        """Test valid category passes."""
        assert validate_category("dining") == "dining"
        assert validate_category("travel") == "travel"

    def test_validate_mcc_valid(self):
        """Test valid MCC code passes."""
        assert validate_mcc_code(5812) == 5812
        assert validate_mcc_code(3000) == 3000

    def test_validate_mcc_invalid(self):
        """Test invalid MCC raises error."""
        with pytest.raises(ValueError):
            validate_mcc_code(999)  # Only 3 digits

        with pytest.raises(ValueError):
            validate_mcc_code(10000)  # 5 digits


class TestFeatureMetadata:
    """Test feature metadata schemas."""

    def test_feature_metadata_valid(self):
        """Test valid feature metadata."""
        meta = FeatureMetadata(
            name="net_value_annual",
            data_type="numeric",
            description="Net annual value",
            source="credit_card",
            nullable=False,
            required_for_ml=True,
            min_value=-1000.0,
            max_value=5000.0,
        )

        assert meta.name == "net_value_annual"
        assert meta.data_type == "numeric"

    def test_feature_registry(self):
        """Test feature registry."""
        registry = FeatureRegistry(
            version="1.0.0",
            credit_card_features=[
                FeatureMetadata(
                    name="base_reward_rate",
                    data_type="numeric",
                    description="Base reward rate",
                    source="credit_card",
                )
            ],
        )

        assert registry.version == "1.0.0"
        assert len(registry.credit_card_features) == 1

        # Test get_feature
        feat = registry.get_feature("base_reward_rate")
        assert feat is not None
        assert feat.name == "base_reward_rate"
