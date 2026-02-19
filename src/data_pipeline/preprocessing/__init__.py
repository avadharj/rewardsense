"""
RewardSense - Data Preprocessing Module

This module provides data cleaning and feature engineering for:
- Credit card data (from scrapers and APIs)
- Transaction data (synthetic or real)
- User profile data (synthetic or real)

Usage:
    from src.data_pipeline.preprocessing import (
        clean_all_data,
        engineer_all_features,
    )

    # Clean all datasets
    clean_cards, clean_txns, clean_users, report = clean_all_data(
        credit_cards_df=cards_df,
        transactions_df=txns_df,
        users_df=users_df,
    )

    # Engineer features
    cards_features, txns_features, users_features = engineer_all_features(
        credit_cards_df=clean_cards,
        transactions_df=clean_txns,
        users_df=clean_users,
    )
"""

# =============================================================================
# Cleaning Module Exports (Story 3.1)
# =============================================================================
from .cleaning import (
    # Configuration
    CleaningConfig,
    DEFAULT_CONFIG,
    # Text normalization utilities
    clean_card_name,
    standardize_issuer_name,
    normalize_welcome_bonus,
    # Main cleaning functions
    clean_credit_card_data,
    clean_transaction_data,
    clean_user_profile_data,
    clean_all_data,
)

# =============================================================================
# Feature Engineering Module Exports (Story 3.2)
# =============================================================================
from .feature_engineering import (
    # Feature engineer classes
    CreditCardFeatureEngineer,
    TransactionFeatureEngineer,
    UserProfileFeatureEngineer,
    # Convenience function
    engineer_all_features,
)

# =============================================================================
# Public API
# =============================================================================
__all__ = [
    # Configuration
    "CleaningConfig",
    "DEFAULT_CONFIG",
    # Text normalization utilities
    "clean_card_name",
    "standardize_issuer_name",
    "normalize_welcome_bonus",
    # Cleaning functions
    "clean_credit_card_data",
    "clean_transaction_data",
    "clean_user_profile_data",
    "clean_all_data",
    # Feature engineering classes
    "CreditCardFeatureEngineer",
    "TransactionFeatureEngineer",
    "UserProfileFeatureEngineer",
    # Feature engineering convenience function
    "engineer_all_features",
]
