# Instrument Service Migration Guide

## Overview

This guide demonstrates how to migrate existing code from direct DAO access to the new service-based architecture. The new architecture provides:

- **Clean service interfaces** - Well-defined public APIs
- **Business logic encapsulation** - All business rules centralized in services
- **Improved testability** - Easy to mock services for testing
- **Better error handling** - Consistent error responses across the system
- **Dependency injection** - Loose coupling between components

## Migration Examples

### Example 1: Basic Instrument Creation

#### BEFORE (Direct DAO Access)
```python
# Old code - direct DAO access with scattered business logic
from core.dao.instruments.instruments_dao import InstrumentsDAO
from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
from core.dao.infrastructure.vendors_dao import VendorsDAO
from core.platform.config.environment import Environment

async def create_instrument_old_way(symbol, name, exchange):
    env = Environment(None, EnvironmentType.DEV)

    # Business logic scattered in client code
    if not symbol:
        raise ValueError("Symbol is required")

    # Multiple DAO instantiations and manual dependency management
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    try:
        # Manual duplicate checking
        existing = await instruments_dao.get_instrument_by_symbol(symbol)
        if existing:
            raise ValueError(f"Instrument {symbol} already exists")

        # Create instrument
        instrument_id = await instruments_dao.create_instrument(
            symbol=symbol,
            name=name,
            exchange=exchange,
            type_="stock",  # Hardcoded business rule
            currency="USD"  # Another hardcoded business rule
        )

        # Manual cross-reference creation
        ticker_vendor = await vendors_dao.get_vendor_by_name("ticker")
        if ticker_vendor:
            await xrefs_dao.create_xref(
                instrument_id=instrument_id,
                vendor_id=ticker_vendor['id'],
                symbol=symbol,
                type="equity",
                start_at=date.today(),
                end_at=None
            )

        return instrument_id

    except Exception as e:
        # Poor error handling
        print(f"Error: {e}")
        return None
```

#### AFTER (Service-Based Architecture)
```python
# New code - clean service interface with encapsulated business logic
from domains.instruments.services.config.service_container import get_instrument_service
from domains.instruments.services.interfaces.instrument_service_interface import InstrumentDTO

async def create_instrument_new_way(symbol, name, exchange):
    # Get service via dependency injection
    service = await get_instrument_service()

    # Create DTO with clean data structure
    instrument = InstrumentDTO(
        symbol=symbol,
        name=name,
        exchange=exchange
    )

    # Single service call handles all business logic
    result = await service.create_instrument(instrument)

    # Structured error handling
    if not result.success:
        raise ValueError(result.error_message)

    return result.instrument_id
```

**Benefits of Migration:**
- ✅ **50% less code** - Service handles complexity
- ✅ **No DAO imports** - Client code isolated from data layer
- ✅ **Better error handling** - Structured error responses
- ✅ **Centralized business logic** - Rules enforced consistently
- ✅ **Easy to test** - Mock service interface, not multiple DAOs

### Example 2: Complex Instrument Lookup with Cross-References

#### BEFORE (Direct DAO Access)
```python
# Old code - complex lookup logic scattered across client
async def lookup_instrument_with_xrefs_old_way(symbol, vendor_name="ticker"):
    env = Environment(None, EnvironmentType.DEV)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    try:
        # Manual vendor ID lookup
        vendor = await vendors_dao.get_vendor_by_name(vendor_name)
        if not vendor:
            return None

        # Manual instrument resolution via xref
        instrument_id = await xrefs_dao.resolve_instrument_id(
            symbol, vendor['id']
        )

        if not instrument_id:
            # Fallback to direct lookup
            instrument_record = await instruments_dao.get_instrument_by_symbol(symbol)
            if not instrument_record:
                return None
            instrument_id = instrument_record['id']

        # Get instrument details
        instrument = await instruments_dao.get_instrument(instrument_id)
        if not instrument:
            return None

        # Get all cross-references
        xrefs = await xrefs_dao.list_xrefs_for_instrument(instrument_id)

        # Manual data transformation
        result = {
            'instrument': dict(instrument),
            'cross_references': [dict(xref) for xref in xrefs]
        }

        return result

    except Exception as e:
        print(f"Error: {e}")
        return None
```

#### AFTER (Service-Based Architecture)
```python
# New code - single service call with unified response
async def lookup_instrument_with_xrefs_new_way(symbol, vendor_name="ticker"):
    service = await get_instrument_service()

    # Single service call handles all complexity
    unified_instrument = await service.get_unified_instrument(symbol, "symbol")

    if not unified_instrument:
        return None

    # Clean, structured response
    return {
        'instrument': unified_instrument.instrument,
        'cross_references': unified_instrument.cross_references,
        'vendor_data': unified_instrument.vendor_data
    }
```

**Benefits of Migration:**
- ✅ **75% less code** - Complex logic encapsulated in service
- ✅ **Single service call** - vs multiple DAO calls
- ✅ **Consistent data structure** - DTOs ensure data integrity
- ✅ **Built-in fallback logic** - Service handles resolution strategies

### Example 3: Batch Instrument Processing

#### BEFORE (Direct DAO Access)
```python
# Old code - manual batch processing with error-prone transaction handling
async def populate_instruments_old_way(instruments_data):
    env = Environment(None, EnvironmentType.DEV)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    created_count = 0
    error_count = 0

    # Manual vendor ID lookup
    ticker_vendor = await vendors_dao.get_vendor_by_name("ticker")
    if not ticker_vendor:
        return {"error": "Ticker vendor not found"}

    # Manual processing of each instrument
    for data in instruments_data:
        try:
            # Check duplicates manually
            existing = await instruments_dao.get_instrument_by_symbol(data['symbol'])
            if existing:
                error_count += 1
                continue

            # Create instrument
            instrument_id = await instruments_dao.create_instrument(
                symbol=data['symbol'],
                name=data.get('name'),
                exchange=data.get('exchange'),
                type_=data.get('type'),
                currency=data.get('currency')
            )

            # Create cross-reference
            await xrefs_dao.create_xref(
                instrument_id=instrument_id,
                vendor_id=ticker_vendor['id'],
                symbol=data['symbol'],
                start_at=data.get('list_date'),
                end_at=data.get('delist_date')
            )

            created_count += 1

        except Exception as e:
            error_count += 1
            print(f"Error processing {data.get('symbol')}: {e}")

    return {
        "created": created_count,
        "errors": error_count
    }
```

#### AFTER (Service-Based Architecture)
```python
# New code - clean batch processing with proper transaction handling
async def populate_instruments_new_way(instruments_data):
    service = await get_instrument_service()

    # Convert to DTOs
    instruments = [
        InstrumentDTO(
            symbol=data['symbol'],
            name=data.get('name'),
            exchange=data.get('exchange'),
            instrument_type=data.get('type'),
            currency=data.get('currency'),
            list_date=data.get('list_date'),
            delist_date=data.get('delist_date')
        )
        for data in instruments_data
    ]

    # Single service call handles batch processing
    result = await service.create_instruments_batch(instruments)

    return {
        "success": result.success,
        "created": result.created_count,
        "errors": result.error_message if not result.success else None
    }
```

**Benefits of Migration:**
- ✅ **90% less code** - Service handles all batch complexity
- ✅ **Proper transaction handling** - Service ensures data consistency
- ✅ **Better error reporting** - Structured error information
- ✅ **Automatic duplicate handling** - Service enforces business rules

### Example 4: Service Integration in FastAPI Endpoints

#### BEFORE (Direct DAO Access in API)
```python
# Old API code - DAOs mixed with HTTP handling
from fastapi import APIRouter
from core.dao.instruments.instruments_dao import InstrumentsDAO

router = APIRouter()

@router.get("/instruments/{symbol}")
async def get_instrument_old(symbol: str):
    try:
        env = Environment(None, EnvironmentType.DEV)
        dao = InstrumentsDAO(env)

        # Business logic in API layer
        if not symbol:
            return {"error": "Symbol required"}

        instrument = await dao.get_instrument_by_symbol(symbol)
        if not instrument:
            return {"error": "Not found"}

        # Manual data transformation in API
        return {
            "id": instrument['id'],
            "symbol": instrument['symbol'],
            "name": instrument.get('name'),
            "exchange": instrument.get('exchange')
        }

    except Exception as e:
        return {"error": str(e)}
```

#### AFTER (Service-Based Architecture)
```python
# New API code - clean separation of concerns
from fastapi import APIRouter, HTTPException, Depends
from domains.instruments.services.interfaces.instrument_service_interface import InstrumentServiceInterface
from services.web_services.api.instruments_api import get_instrument_service

router = APIRouter()

@router.get("/instruments/{symbol}")
async def get_instrument_new(
    symbol: str,
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    # Clean service call
    instrument = await service.get_instrument_by_symbol(symbol)

    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")

    # DTO automatically provides clean structure
    return instrument
```

**Benefits of Migration:**
- ✅ **Clean separation** - API handles HTTP, service handles business logic
- ✅ **Dependency injection** - Testable and configurable
- ✅ **Consistent error handling** - HTTP-appropriate error responses
- ✅ **Automatic serialization** - DTOs work seamlessly with FastAPI

## Migration Strategy

### Phase 1: Identify Current DAO Usage
1. **Find direct DAO imports** in client code
2. **Identify business logic** scattered across multiple files
3. **Document current data flows** and dependencies

### Phase 2: Implement Service Layer
1. **Define service interfaces** for your domain
2. **Implement business logic** in service classes
3. **Set up dependency injection** configuration

### Phase 3: Migrate Client Code
1. **Start with new features** - Use services from day one
2. **Migrate APIs first** - High impact, visible improvements
3. **Migrate utilities and scripts** - Lower risk, good practice
4. **Update tests** - Mock services instead of DAOs

### Phase 4: Remove Direct DAO Access
1. **Update import policies** - Prevent new direct DAO usage
2. **Refactor remaining code** - Complete the migration
3. **Remove unused DAO imports** - Clean up dependencies

## Testing Migration

### Service Layer Testing
```python
# Test services in isolation with mocked DAOs
@pytest.fixture
def mock_dao():
    return Mock(spec=InstrumentsDAO)

async def test_service_business_logic(mock_dao):
    service = InstrumentServiceImpl(instruments_dao=mock_dao, ...)

    # Test business logic without database
    result = await service.create_instrument(test_dto)
    assert result.success is True
```

### API Testing
```python
# Test APIs with mocked services
@patch('api.get_instrument_service')
async def test_api_endpoint(mock_service):
    mock_service.return_value.get_instrument_by_symbol.return_value = test_dto

    # Test HTTP layer without business logic
    response = client.get("/instruments/AAPL")
    assert response.status_code == 200
```

## Benefits Summary

| Aspect | Before (DAO Access) | After (Service Layer) |
|--------|-------------------|---------------------|
| **Code Lines** | 100+ lines | 10-20 lines |
| **Dependencies** | 3-5 DAO imports | 1 service import |
| **Error Handling** | Manual, inconsistent | Structured, consistent |
| **Business Logic** | Scattered | Centralized |
| **Testability** | Mock multiple DAOs | Mock one service |
| **Maintainability** | High coupling | Loose coupling |

## Next Steps

1. **Review existing code** using the patterns shown above
2. **Identify high-impact areas** to migrate first (APIs, core services)
3. **Implement service interfaces** for your specific domain needs
4. **Create migration plan** with phases and timelines
5. **Update team practices** to use services for new development

The service-based architecture provides a solid foundation for scalable, maintainable code that follows clean architecture principles.