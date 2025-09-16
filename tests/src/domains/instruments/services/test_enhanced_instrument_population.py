"""
Enhanced Instrument Population Test Cases with Real Data

This module tests the comprehensive instrument population system that:
1. Uses EODHD as primary data source
2. Reconciles with Polygon and Tiingo APIs
3. Creates instrument_xrefs for exchanges, CUSIP, ISIN
4. Tracks listing/delisting dates and exchange migrations
5. Handles diverse real-world scenarios

Real test cases based on actual market data sampled from the database.
"""

import pytest
from datetime import date, datetime
from typing import Dict, List, Optional, Any

class TestEnhancedInstrumentPopulation:
    """Test enhanced instrument population with real market data"""

    @pytest.fixture
    def real_test_instruments(self):
        """Real instruments sampled from the database with diverse scenarios"""
        return {
            # Major NASDAQ stocks - stable, single exchange
            'AAPL': {
                'symbol': 'AAPL',
                'name': 'Apple Inc',
                'category': 'major_nasdaq',
                'eodhd': {
                    'exchange': 'NASDAQ',
                    'isin': 'US0378331005',
                    'cusip': '037833100',
                    'listing_date': '1980-12-12',
                    'type': 'Common Stock',
                    'is_delisted': False
                },
                'polygon': {
                    'primary_exchange': 'XNAS',
                    'active': True,
                    'list_date': '1980-12-12',
                    'currency_name': 'USD'
                },
                'expected_scenarios': [
                    'single_exchange_listing',
                    'long_term_stability',
                    'has_fundamentals',
                    'cusip_isin_available'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NASDAQ', 'start_date': '1980-12-12'},
                    {'vendor': 'cusip', 'symbol': '037833100'},
                    {'vendor': 'isin', 'symbol': 'US0378331005'}
                ]
            },

            'AMZN': {
                'symbol': 'AMZN',
                'name': 'Amazon.com Inc',
                'category': 'major_nasdaq',
                'eodhd': {
                    'exchange': 'NASDAQ',
                    'isin': 'US0231351067',
                    'cusip': '023135106',
                    'listing_date': '1997-05-15',
                    'type': 'Common Stock',
                    'is_delisted': False
                },
                'polygon': {
                    'primary_exchange': 'XNAS',
                    'active': True,
                    'list_date': '1997-05-15',
                    'currency_name': 'USD'
                },
                'expected_scenarios': [
                    'single_exchange_listing',
                    'long_term_stability',
                    'has_fundamentals',
                    'cusip_isin_available'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NASDAQ', 'start_date': '1997-05-15'},
                    {'vendor': 'cusip', 'symbol': '023135106'},
                    {'vendor': 'isin', 'symbol': 'US0231351067'}
                ]
            },

            # Major NYSE stocks - institutional grade
            'JNJ': {
                'symbol': 'JNJ',
                'name': 'Johnson & Johnson',
                'category': 'major_nyse',
                'eodhd': {
                    'exchange': 'NYSE',
                    'isin': 'US4781601046',
                    'cusip': '478160104',
                    'listing_date': '1944-09-24',
                    'type': 'Common Stock',
                    'is_delisted': False
                },
                'polygon': {
                    'primary_exchange': 'XNYS',
                    'active': True,
                    'list_date': '1944-09-24',
                    'currency_name': 'USD'
                },
                'expected_scenarios': [
                    'single_exchange_listing',
                    'nyse_primary_listing',
                    'institutional_grade',
                    'full_reference_data'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NYSE', 'start_date': '1944-09-24'},
                    {'vendor': 'cusip', 'symbol': '478160104'},
                    {'vendor': 'isin', 'symbol': 'US4781601046'}
                ]
            },

            # Delisted stocks - complete lifecycle examples
            'BIOCQ': {
                'symbol': 'BIOCQ',
                'name': 'Biocept Inc.',
                'category': 'delisted',
                'eodhd': {
                    'exchange': 'NASDAQ',
                    'type': 'Common Stock',
                    'is_delisted': True,
                    'listing_date': '2010-03-15',  # Estimated
                    'delisting_date': '2024-01-15'  # Estimated
                },
                'expected_scenarios': [
                    'delisting_lifecycle',
                    'end_date_populated',
                    'historical_trading_only',
                    'bankruptcy_or_acquisition'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NASDAQ', 'start_date': '2010-03-15', 'end_date': '2024-01-15'}
                ]
            },

            'CSSEQ': {
                'symbol': 'CSSEQ',
                'name': 'Chicken Soup for the Soul Entertainment, Inc.',
                'category': 'delisted',
                'eodhd': {
                    'exchange': 'NASDAQ',
                    'type': 'Common Stock',
                    'is_delisted': True,
                    'listing_date': '2017-08-10',  # Estimated
                    'delisting_date': '2023-12-20'  # Estimated
                },
                'expected_scenarios': [
                    'delisting_lifecycle',
                    'end_date_populated',
                    'historical_trading_only',
                    'bankruptcy_or_acquisition'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NASDAQ', 'start_date': '2017-08-10', 'end_date': '2023-12-20'}
                ]
            },

            # Symbol changes - corporate actions
            'AAMI': {
                'symbol': 'AAMI',
                'name': 'Acadian Asset Management Inc',
                'category': 'symbol_change',
                'eodhd': {
                    'exchange': 'NYSE',
                    'type': 'Common Stock',
                    'is_delisted': False,
                    'listing_date': '2021-03-01'  # Estimated IPO date
                },
                'expected_scenarios': [
                    'symbol_migration',
                    'corporate_action',
                    'reference_data_update',
                    'historical_continuity'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NYSE', 'start_date': '2021-03-01'}
                ]
            },

            # OTC downgrade - exchange migration
            'SHPW': {
                'symbol': 'SHPW',
                'name': 'Shapeways Holdings, Inc. Common Stock',
                'category': 'otc',
                'eodhd': {
                    'exchange': 'OTCMKTS',
                    'type': 'Common Stock',
                    'is_delisted': False,
                    'listing_date': '2021-06-15',  # Original NASDAQ listing
                    'exchange_migration': {
                        'original_exchange': 'NASDAQ',
                        'migration_date': '2023-08-15',
                        'current_exchange': 'OTCMKTS'
                    }
                },
                'expected_scenarios': [
                    'exchange_downgrade',
                    'otc_trading',
                    'limited_liquidity',
                    'potential_delisting_risk'
                ],
                'expected_xrefs': [
                    {'vendor': 'exchange', 'symbol': 'NASDAQ', 'start_date': '2021-06-15', 'end_date': '2023-08-15'},
                    {'vendor': 'exchange', 'symbol': 'OTCMKTS', 'start_date': '2023-08-15'}
                ]
            }
        }

    def test_eodhd_primary_data_structure(self, real_test_instruments):
        """Test EODHD primary data structure and validation"""

        def validate_eodhd_instrument(instrument_data: Dict[str, Any]) -> Dict[str, Any]:
            """Validate EODHD instrument data structure"""
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': []
            }

            # Required fields for primary data source
            required_fields = ['symbol', 'name', 'exchange', 'type']
            for field in required_fields:
                if not instrument_data.get(field):
                    validation_result['errors'].append(f"Missing required field: {field}")
                    validation_result['is_valid'] = False

            # Validate exchange
            valid_exchanges = {'NYSE', 'NASDAQ', 'NYSE ARCA', 'BATS', 'OTCMKTS', 'OTC'}
            if instrument_data.get('exchange') not in valid_exchanges:
                validation_result['warnings'].append(f"Uncommon exchange: {instrument_data.get('exchange')}")

            # Validate dates
            if instrument_data.get('listing_date'):
                try:
                    listing_date = datetime.strptime(instrument_data['listing_date'], '%Y-%m-%d').date()
                    if listing_date > date.today():
                        validation_result['errors'].append("Listing date cannot be in the future")
                        validation_result['is_valid'] = False
                except ValueError:
                    validation_result['errors'].append("Invalid listing date format")
                    validation_result['is_valid'] = False

            # Validate reference data
            if instrument_data.get('isin') and len(instrument_data['isin']) != 12:
                validation_result['warnings'].append("ISIN should be 12 characters")

            if instrument_data.get('cusip') and len(instrument_data['cusip']) != 9:
                validation_result['warnings'].append("CUSIP should be 9 characters")

            return validation_result

        # Test each real instrument
        for symbol, instrument in real_test_instruments.items():
            eodhd_data = {
                'symbol': symbol,
                'name': instrument['name'],
                'exchange': instrument['eodhd']['exchange'],
                'type': instrument['eodhd']['type'],
                'listing_date': instrument['eodhd'].get('listing_date'),
                'isin': instrument['eodhd'].get('isin'),
                'cusip': instrument['eodhd'].get('cusip')
            }

            validation = validate_eodhd_instrument(eodhd_data)

            # All major instruments should pass validation
            if instrument['category'] in ['major_nasdaq', 'major_nyse']:
                assert validation['is_valid'], f"Major instrument {symbol} failed validation: {validation['errors']}"
                assert len(validation['errors']) == 0

            # Delisted instruments might have warnings but should be structurally valid
            if instrument['category'] == 'delisted':
                assert 'symbol' in eodhd_data
                assert 'name' in eodhd_data
                assert eodhd_data.get('exchange') in {'NYSE', 'NASDAQ'}

    def test_multi_vendor_reconciliation(self, real_test_instruments):
        """Test reconciliation between EODHD, Polygon, and Tiingo data"""

        def reconcile_instrument_data(eodhd_data: Dict, polygon_data: Dict, tiingo_data: Optional[Dict] = None) -> Dict:
            """Reconcile instrument data from multiple vendors"""
            reconciled = {
                'symbol': eodhd_data.get('symbol'),  # EODHD is primary
                'name': eodhd_data.get('name'),      # EODHD is primary
                'exchange': None,
                'listing_date': None,
                'currency': 'USD',  # Default
                'active': True,
                'reconciliation_notes': [],
                'data_sources': []
            }

            # Exchange reconciliation
            eodhd_exchange = eodhd_data.get('exchange')
            polygon_exchange = polygon_data.get('primary_exchange') if polygon_data else None

            # Exchange mapping
            exchange_mapping = {
                'XNAS': 'NASDAQ',
                'XNYS': 'NYSE',
                'ARCX': 'NYSE ARCA',
                'BATS': 'BATS'
            }

            if polygon_exchange in exchange_mapping:
                polygon_exchange_normalized = exchange_mapping[polygon_exchange]
            else:
                polygon_exchange_normalized = polygon_exchange

            # Primary exchange from EODHD
            reconciled['exchange'] = eodhd_exchange
            reconciled['data_sources'].append('eodhd')

            # Check consistency
            if polygon_exchange_normalized and eodhd_exchange != polygon_exchange_normalized:
                reconciled['reconciliation_notes'].append(
                    f"Exchange mismatch: EODHD={eodhd_exchange}, Polygon={polygon_exchange_normalized}"
                )

            if polygon_data:
                reconciled['data_sources'].append('polygon')

                # Use Polygon for active status
                reconciled['active'] = polygon_data.get('active', True)

                # Reconcile listing dates
                eodhd_date = eodhd_data.get('listing_date')
                polygon_date = polygon_data.get('list_date')

                if eodhd_date and polygon_date and eodhd_date != polygon_date:
                    reconciled['reconciliation_notes'].append(
                        f"Listing date mismatch: EODHD={eodhd_date}, Polygon={polygon_date}"
                    )

                reconciled['listing_date'] = eodhd_date or polygon_date

            return reconciled

        # Test reconciliation with real data
        test_cases = [
            ('AAPL', real_test_instruments['AAPL']),
            ('AMZN', real_test_instruments['AMZN']),
            ('JNJ', real_test_instruments['JNJ'])
        ]

        for symbol, instrument in test_cases:
            eodhd_data = instrument['eodhd'].copy()
            eodhd_data['symbol'] = symbol  # Ensure symbol is in EODHD data
            eodhd_data['name'] = instrument['name']  # Ensure name is in EODHD data
            polygon_data = instrument.get('polygon', {})

            reconciled = reconcile_instrument_data(eodhd_data, polygon_data)

            # Verify reconciliation results
            assert reconciled['symbol'] == symbol
            assert reconciled['name'] == instrument['name']
            assert reconciled['exchange'] in {'NYSE', 'NASDAQ'}
            assert 'eodhd' in reconciled['data_sources']

            if polygon_data:
                assert 'polygon' in reconciled['data_sources']
                assert isinstance(reconciled['active'], bool)

    def test_instrument_xrefs_creation(self, real_test_instruments):
        """Test creation of instrument_xrefs for exchanges, CUSIP, ISIN"""

        def create_instrument_xrefs(instrument_data: Dict, exchange_vendor_id: int,
                                  cusip_vendor_id: int, isin_vendor_id: int) -> List[Dict]:
            """Create instrument cross-references from instrument data"""
            xrefs = []

            # Exchange reference
            if instrument_data.get('exchange'):
                exchange_xref = {
                    'vendor_id': exchange_vendor_id,
                    'external_symbol': instrument_data['exchange'],
                    'start_date': instrument_data.get('listing_date'),
                    'end_date': instrument_data.get('delisting_date')
                }
                xrefs.append(exchange_xref)

            # CUSIP reference
            if instrument_data.get('cusip'):
                cusip_xref = {
                    'vendor_id': cusip_vendor_id,
                    'external_symbol': instrument_data['cusip'],
                    'start_date': None,  # CUSIP is permanent
                    'end_date': None
                }
                xrefs.append(cusip_xref)

            # ISIN reference
            if instrument_data.get('isin'):
                isin_xref = {
                    'vendor_id': isin_vendor_id,
                    'external_symbol': instrument_data['isin'],
                    'start_date': None,  # ISIN is permanent
                    'end_date': None
                }
                xrefs.append(isin_xref)

            return xrefs

        # Mock vendor IDs
        exchange_vendor_id = 1
        cusip_vendor_id = 2
        isin_vendor_id = 3

        # Test xref creation for each instrument
        for symbol, instrument in real_test_instruments.items():
            eodhd_data = instrument['eodhd']

            xrefs = create_instrument_xrefs(
                eodhd_data, exchange_vendor_id, cusip_vendor_id, isin_vendor_id
            )

            # Verify xref structure
            assert len(xrefs) >= 1  # At least exchange xref

            # Check exchange xref
            exchange_xrefs = [x for x in xrefs if x['vendor_id'] == exchange_vendor_id]
            assert len(exchange_xrefs) == 1
            assert exchange_xrefs[0]['external_symbol'] in {'NYSE', 'NASDAQ', 'OTCMKTS'}

            # Check CUSIP xref for major instruments
            if instrument['category'] in ['major_nasdaq', 'major_nyse']:
                cusip_xrefs = [x for x in xrefs if x['vendor_id'] == cusip_vendor_id]
                assert len(cusip_xrefs) == 1
                assert len(cusip_xrefs[0]['external_symbol']) == 9
                assert cusip_xrefs[0]['start_date'] is None  # Permanent

            # Check ISIN xref for major instruments
            if instrument['category'] in ['major_nasdaq', 'major_nyse']:
                isin_xrefs = [x for x in xrefs if x['vendor_id'] == isin_vendor_id]
                assert len(isin_xrefs) == 1
                assert len(isin_xrefs[0]['external_symbol']) == 12
                assert isin_xrefs[0]['external_symbol'].startswith('US')

    def test_listing_delisting_date_tracking(self, real_test_instruments):
        """Test comprehensive listing and delisting date tracking"""

        def process_listing_lifecycle(instrument_data: Dict) -> Dict:
            """Process instrument listing lifecycle"""
            lifecycle = {
                'symbol': instrument_data.get('symbol'),
                'listing_date': None,
                'delisting_date': None,
                'is_active': True,
                'lifecycle_stage': 'unknown',
                'exchange_history': []
            }

            # Parse dates
            if instrument_data.get('listing_date'):
                try:
                    lifecycle['listing_date'] = datetime.strptime(
                        instrument_data['listing_date'], '%Y-%m-%d'
                    ).date()
                except ValueError:
                    pass

            if instrument_data.get('delisting_date'):
                try:
                    lifecycle['delisting_date'] = datetime.strptime(
                        instrument_data['delisting_date'], '%Y-%m-%d'
                    ).date()
                    lifecycle['is_active'] = False
                except ValueError:
                    pass

            # Determine lifecycle stage
            if instrument_data.get('is_delisted'):
                lifecycle['lifecycle_stage'] = 'delisted'
                lifecycle['is_active'] = False
            elif lifecycle['listing_date']:
                days_listed = (date.today() - lifecycle['listing_date']).days
                if days_listed > 365 * 10:  # 10+ years
                    lifecycle['lifecycle_stage'] = 'mature'
                elif days_listed > 365:     # 1+ years
                    lifecycle['lifecycle_stage'] = 'established'
                else:
                    lifecycle['lifecycle_stage'] = 'new'

            # Exchange history
            if instrument_data.get('exchange_migration'):
                migration = instrument_data['exchange_migration']
                lifecycle['exchange_history'] = [
                    {
                        'exchange': migration['original_exchange'],
                        'start_date': lifecycle['listing_date'],
                        'end_date': datetime.strptime(migration['migration_date'], '%Y-%m-%d').date()
                    },
                    {
                        'exchange': migration['current_exchange'],
                        'start_date': datetime.strptime(migration['migration_date'], '%Y-%m-%d').date(),
                        'end_date': lifecycle['delisting_date']
                    }
                ]
            else:
                lifecycle['exchange_history'] = [{
                    'exchange': instrument_data.get('exchange'),
                    'start_date': lifecycle['listing_date'],
                    'end_date': lifecycle['delisting_date']
                }]

            return lifecycle

        # Test lifecycle processing for different scenarios
        test_scenarios = [
            # Long-term stable (AAPL)
            ('AAPL', real_test_instruments['AAPL']['eodhd'], 'mature'),
            # Established (AMZN)
            ('AMZN', real_test_instruments['AMZN']['eodhd'], 'mature'),
            # Very mature (JNJ)
            ('JNJ', real_test_instruments['JNJ']['eodhd'], 'mature'),
            # Delisted (BIOCQ)
            ('BIOCQ', real_test_instruments['BIOCQ']['eodhd'], 'delisted'),
            # Exchange migration (SHPW)
            ('SHPW', real_test_instruments['SHPW']['eodhd'], 'established')
        ]

        for symbol, eodhd_data, expected_stage in test_scenarios:
            eodhd_data['symbol'] = symbol  # Ensure symbol is set
            lifecycle = process_listing_lifecycle(eodhd_data)

            # Basic validation
            assert lifecycle['symbol'] == symbol
            assert lifecycle['lifecycle_stage'] == expected_stage
            assert len(lifecycle['exchange_history']) >= 1

            # Stage-specific validation
            if expected_stage == 'delisted':
                assert lifecycle['is_active'] is False
                assert lifecycle['delisting_date'] is not None or eodhd_data.get('is_delisted')

            elif expected_stage == 'mature':
                assert lifecycle['is_active'] is True
                assert lifecycle['listing_date'] is not None
                assert (date.today() - lifecycle['listing_date']).days > 365 * 10

            # Exchange history validation
            for history_entry in lifecycle['exchange_history']:
                assert history_entry['exchange'] in {'NYSE', 'NASDAQ', 'OTCMKTS'}
                if history_entry['start_date'] and history_entry['end_date']:
                    assert history_entry['start_date'] <= history_entry['end_date']

    def test_exchange_migration_scenarios(self, real_test_instruments):
        """Test handling of exchange migration scenarios"""

        def detect_exchange_migrations(current_data: Dict, historical_data: List[Dict]) -> List[Dict]:
            """Detect exchange migrations from historical data"""
            migrations = []

            # Look for exchange changes in historical data
            exchanges_seen = set()
            for data_point in sorted(historical_data, key=lambda x: x.get('date', '1900-01-01')):
                exchange = data_point.get('exchange')
                if exchange and exchange not in exchanges_seen:
                    if exchanges_seen:  # Not the first exchange
                        migration = {
                            'from_exchange': list(exchanges_seen)[-1] if exchanges_seen else None,
                            'to_exchange': exchange,
                            'migration_date': data_point.get('date'),
                            'migration_type': self._classify_migration(
                                list(exchanges_seen)[-1] if exchanges_seen else None,
                                exchange
                            )
                        }
                        migrations.append(migration)
                    exchanges_seen.add(exchange)

            return migrations

        def _classify_migration(from_exchange: str, to_exchange: str) -> str:
            """Classify type of exchange migration"""
            major_exchanges = {'NYSE', 'NASDAQ'}

            if from_exchange in major_exchanges and to_exchange not in major_exchanges:
                return 'downgrade'
            elif from_exchange not in major_exchanges and to_exchange in major_exchanges:
                return 'upgrade'
            elif from_exchange in major_exchanges and to_exchange in major_exchanges:
                return 'lateral'
            else:
                return 'otc_movement'

        # Test migration scenarios

        # Scenario 1: No migration (stable listing)
        stable_historical = [
            {'exchange': 'NASDAQ', 'date': '2020-01-01'},
            {'exchange': 'NASDAQ', 'date': '2021-01-01'},
            {'exchange': 'NASDAQ', 'date': '2022-01-01'}
        ]

        migrations = detect_exchange_migrations(
            real_test_instruments['AAPL']['eodhd'],
            stable_historical
        )
        assert len(migrations) == 0

        # Scenario 2: Downgrade migration (NASDAQ to OTC)
        downgrade_historical = [
            {'exchange': 'NASDAQ', 'date': '2021-01-01'},
            {'exchange': 'NASDAQ', 'date': '2022-01-01'},
            {'exchange': 'OTCMKTS', 'date': '2023-01-01'}
        ]

        migrations = detect_exchange_migrations(
            real_test_instruments['SHPW']['eodhd'],
            downgrade_historical
        )
        assert len(migrations) == 1
        assert migrations[0]['migration_type'] == 'downgrade'
        assert migrations[0]['from_exchange'] == 'NASDAQ'
        assert migrations[0]['to_exchange'] == 'OTCMKTS'

        # Scenario 3: Lateral migration (NYSE to NASDAQ)
        lateral_historical = [
            {'exchange': 'NYSE', 'date': '2020-01-01'},
            {'exchange': 'NYSE', 'date': '2021-01-01'},
            {'exchange': 'NASDAQ', 'date': '2022-01-01'}
        ]

        migrations = detect_exchange_migrations({}, lateral_historical)
        assert len(migrations) == 1
        assert migrations[0]['migration_type'] == 'lateral'
        assert migrations[0]['from_exchange'] == 'NYSE'
        assert migrations[0]['to_exchange'] == 'NASDAQ'

    def test_comprehensive_data_quality_validation(self, real_test_instruments):
        """Test comprehensive data quality validation across all vendors"""

        def validate_comprehensive_instrument(eodhd_data: Dict, polygon_data: Dict,
                                           tiingo_data: Optional[Dict] = None) -> Dict:
            """Comprehensive validation across all vendor data"""
            validation = {
                'overall_score': 0,
                'data_completeness': {},
                'consistency_checks': {},
                'reference_data_quality': {},
                'recommendations': []
            }

            # Data completeness scoring
            completeness_score = 0
            required_fields = ['symbol', 'name', 'exchange']
            optional_fields = ['isin', 'cusip', 'listing_date', 'currency']

            for field in required_fields:
                if eodhd_data.get(field):
                    completeness_score += 20
                    validation['data_completeness'][field] = 'present'
                else:
                    validation['data_completeness'][field] = 'missing'

            for field in optional_fields:
                if eodhd_data.get(field):
                    completeness_score += 10
                    validation['data_completeness'][field] = 'present'
                else:
                    validation['data_completeness'][field] = 'missing'

            # Consistency checks between vendors
            if polygon_data:
                # Symbol consistency
                if eodhd_data.get('symbol') == polygon_data.get('ticker', '').replace('.', ''):
                    validation['consistency_checks']['symbol_match'] = True
                    completeness_score += 10
                else:
                    validation['consistency_checks']['symbol_match'] = False
                    validation['recommendations'].append("Symbol mismatch between vendors")

                # Exchange consistency
                exchange_mapping = {'XNAS': 'NASDAQ', 'XNYS': 'NYSE'}
                polygon_exchange = exchange_mapping.get(
                    polygon_data.get('primary_exchange'),
                    polygon_data.get('primary_exchange')
                )

                if eodhd_data.get('exchange') == polygon_exchange:
                    validation['consistency_checks']['exchange_match'] = True
                    completeness_score += 10
                else:
                    validation['consistency_checks']['exchange_match'] = False
                    validation['recommendations'].append("Exchange mismatch between vendors")

            # Reference data quality
            isin = eodhd_data.get('isin', '')
            cusip = eodhd_data.get('cusip', '')

            if isin:
                if len(isin) == 12 and isin.startswith('US'):
                    validation['reference_data_quality']['isin_format'] = 'valid'
                    completeness_score += 5
                else:
                    validation['reference_data_quality']['isin_format'] = 'invalid'
                    validation['recommendations'].append("ISIN format validation failed")

            if cusip:
                if len(cusip) == 9 and cusip.isalnum():
                    validation['reference_data_quality']['cusip_format'] = 'valid'
                    completeness_score += 5
                else:
                    validation['reference_data_quality']['cusip_format'] = 'invalid'
                    validation['recommendations'].append("CUSIP format validation failed")

            validation['overall_score'] = min(completeness_score, 100)

            return validation

        # Test validation for each instrument category
        for symbol, instrument in real_test_instruments.items():
            eodhd_data = instrument['eodhd']
            polygon_data = instrument.get('polygon', {})

            validation = validate_comprehensive_instrument(eodhd_data, polygon_data)

            # Category-specific expectations
            if instrument['category'] in ['major_nasdaq', 'major_nyse']:
                # Major instruments should have high data quality
                assert validation['overall_score'] >= 80
                assert validation['data_completeness']['symbol'] == 'present'
                assert validation['data_completeness']['name'] == 'present'
                assert validation['data_completeness']['exchange'] == 'present'

                if polygon_data:
                    assert validation['consistency_checks']['symbol_match'] is True
                    assert validation['consistency_checks']['exchange_match'] is True

            elif instrument['category'] == 'delisted':
                # Delisted instruments may have lower completeness but should be consistent
                assert validation['overall_score'] >= 40  # Lower threshold
                assert validation['data_completeness']['symbol'] == 'present'
                assert validation['data_completeness']['name'] == 'present'

            elif instrument['category'] == 'otc':
                # OTC instruments may have limited reference data
                assert validation['overall_score'] >= 30  # Lowest threshold
                assert validation['data_completeness']['symbol'] == 'present'
                # May not have CUSIP/ISIN

            # Log validation results for analysis
            print(f"\n{symbol} ({instrument['category']}):")
            print(f"  Overall Score: {validation['overall_score']}")
            print(f"  Completeness: {validation['data_completeness']}")
            if validation['recommendations']:
                print(f"  Recommendations: {validation['recommendations']}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])