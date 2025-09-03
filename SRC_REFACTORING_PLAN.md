# 🏗️ ULTRA-AGGRESSIVE SRC REFACTORING PLAN

## 🎯 **OBJECTIVE: CLEAN ARCHITECTURE + DOMAIN-DRIVEN DESIGN**

Transform the sprawling 100+ file `src/` directory into a well-defined, maintainable architecture following Domain-Driven Design principles and Clean Architecture patterns.

## 🔍 **CURRENT ISSUES ANALYSIS**

### **1. Architectural Anti-Patterns**
- ❌ **DAO Sprawl**: 20+ DAO files scattered without domain boundaries
- ❌ **Service Chaos**: Multiple analytics services, unclear responsibilities  
- ❌ **Mixed Abstractions**: Infrastructure mixed with domain logic
- ❌ **Deep Nesting**: Paths like `market_data/agent/monitoring/dashboards/`
- ❌ **Duplicate Logic**: Similar patterns repeated across modules
- ❌ **Unclear Boundaries**: No clear separation between domains

### **2. Quantified Complexity**
- **100+ Python files** in src/ directory
- **15+ subdirectories** at varying depths  
- **Multiple similar modules**: 5+ analytics variants, 8+ data ingestion patterns
- **Inconsistent naming**: Mixed conventions across modules
- **Circular dependencies**: Implied by scattered organization

## 🏛️ **TARGET ARCHITECTURE: DOMAIN-DRIVEN + CLEAN ARCHITECTURE**

### **Core Principles**
1. **Domain-First Design**: Business domains as primary organization
2. **Dependency Inversion**: High-level modules don't depend on low-level details
3. **Single Responsibility**: Each module has one clear purpose
4. **Interface Segregation**: Minimal, focused interfaces
5. **Clear Boundaries**: Explicit domain and layer boundaries

### **Target Structure**
```
src/
├── domains/                    # 🏢 CORE BUSINESS DOMAINS
│   ├── market_data/           # Market data collection, storage, validation
│   │   ├── entities/          # Market data domain models
│   │   ├── repositories/      # Data access interfaces
│   │   ├── services/          # Domain business logic
│   │   └── value_objects/     # Market data value objects
│   ├── instruments/           # Securities, exchanges, corporate actions
│   │   ├── entities/          # Instrument, Exchange models
│   │   ├── repositories/      # Instrument data access
│   │   ├── services/          # Security master logic
│   │   └── value_objects/     # Symbol, ISIN, etc.
│   ├── trading/               # Portfolio, positions, signals
│   │   ├── entities/          # Portfolio, Position models  
│   │   ├── repositories/      # Trading data access
│   │   ├── services/          # Trading business logic
│   │   └── value_objects/     # Money, Quantity, etc.
│   ├── analytics/             # Research, analysis, reporting
│   │   ├── entities/          # Report, Analysis models
│   │   ├── repositories/      # Analytics data access
│   │   ├── services/          # Analytics business logic
│   │   └── value_objects/     # Metrics, Statistics
│   └── ml/                    # Machine learning, forecasting
│       ├── entities/          # Model, Dataset, Feature models
│       ├── repositories/      # ML data access
│       ├── services/          # ML training, prediction logic
│       └── value_objects/     # ModelScore, Prediction, etc.
│
├── application/                # 🚀 APPLICATION SERVICES & USE CASES
│   ├── commands/              # Write operations (CQRS)
│   ├── queries/               # Read operations (CQRS)
│   ├── orchestrators/         # Complex multi-domain workflows
│   └── validators/            # Cross-domain validation
│
├── infrastructure/            # 🔧 EXTERNAL CONCERNS
│   ├── database/              # Database implementations
│   │   ├── migrations/        # Database schema migrations
│   │   ├── repositories/      # Concrete repository implementations
│   │   └── connection/        # Connection management
│   ├── external_apis/         # Third-party API clients
│   │   ├── polygon/           # Polygon.io integration
│   │   ├── tiingo/            # Tiingo integration
│   │   └── eodhd/             # EODHD integration
│   ├── monitoring/            # Logging, metrics, health checks
│   ├── storage/               # File storage, caching
│   └── messaging/             # Event publishing, notifications
│
├── interfaces/                # 🌐 EXTERNAL INTERFACES
│   ├── rest_api/              # REST API controllers
│   ├── cli/                   # Command-line interfaces
│   ├── web_ui/                # Web UI controllers
│   └── event_handlers/        # Message/event handlers
│
└── shared/                    # 🔗 SHARED UTILITIES & TYPES
    ├── types/                 # Common type definitions
    ├── exceptions/            # Common exceptions
    ├── utils/                 # Pure utility functions
    └── constants/             # Application constants
```

## 📊 **REFACTORING PHASES**

### **Phase 1: Domain Extraction (HIGH IMPACT)**
**Target**: Extract core business domains from scattered files

**Market Data Domain:**
- **From**: `market_data/`, `data_ingestion/`, `dao/daily_prices_*`
- **To**: `domains/market_data/`
- **Files**: 25+ files → organized domain structure

**Instruments Domain:**
- **From**: `secmaster/`, `dao/instruments*`, `dao/instrument_*`
- **To**: `domains/instruments/`
- **Files**: 15+ files → organized domain structure

**Trading Domain:**
- **From**: `portfolio/`, `signals/`, `universe/`
- **To**: `domains/trading/`
- **Files**: 20+ files → organized domain structure

**Analytics Domain:**
- **From**: `analytics/`, `services/analytics/`, `dao/*analytics*`
- **To**: `domains/analytics/`
- **Files**: 10+ files → unified domain

**ML Domain:**
- **From**: `ml/`, `modeling/`, `training/`
- **To**: `domains/ml/`
- **Files**: 15+ files → organized ML pipeline

### **Phase 2: Infrastructure Consolidation**
**Target**: Separate infrastructure concerns from business logic

**Database Layer:**
- **From**: `db/`, `dao/`, scattered database code
- **To**: `infrastructure/database/`
- **Impact**: Single source of truth for data access

**External APIs:**
- **From**: Scattered vendor clients
- **To**: `infrastructure/external_apis/`
- **Impact**: Unified API client management

**Monitoring & Storage:**
- **From**: `monitoring/`, `storage/`, logging code
- **To**: `infrastructure/monitoring/`, `infrastructure/storage/`
- **Impact**: Clean separation of concerns

### **Phase 3: Application Layer Organization**
**Target**: Create clear application services layer

**CQRS Pattern:**
- **Commands**: Write operations across domains
- **Queries**: Read operations with optimized data access
- **Orchestrators**: Complex multi-domain workflows

### **Phase 4: Interface Layer Extraction**
**Target**: Extract all external interfaces

**API Layer:**
- **From**: `api/`, `current_portfolio_api.py`, `analytics_api_dynamic.py`
- **To**: `interfaces/rest_api/`

**CLI Layer:**
- **From**: CLI-related code in various modules
- **To**: `interfaces/cli/`

## 🎯 **EXPECTED BENEFITS**

### **Architectural Quality**
- **80% reduction** in architectural complexity
- **Clear domain boundaries** with explicit interfaces
- **Testable architecture** with dependency injection
- **Scalable design** supporting future growth

### **Developer Experience**
- **Faster navigation**: Clear domain-based organization
- **Easier onboarding**: Architecture matches business concepts
- **Reduced coupling**: Domains can evolve independently
- **Clear ownership**: Each domain has focused responsibility

### **Maintainability**
- **Single responsibility**: Each module has one clear purpose
- **Dependency inversion**: Easy to mock and test
- **Interface segregation**: Minimal, focused contracts
- **Open/closed principle**: Easy to extend without modification

## 🚀 **EXECUTION STRATEGY**

### **Safety Measures**
1. **Incremental migration**: Move one domain at a time
2. **Preserve functionality**: All existing features maintained
3. **Update imports systematically**: Automated import updates
4. **Comprehensive testing**: Validate each phase

### **Validation Criteria**
- [ ] **All tests pass** after each phase
- [ ] **No broken imports** across the codebase
- [ ] **Clear domain boundaries** with explicit interfaces
- [ ] **Consistent naming conventions** throughout
- [ ] **Documentation updated** to reflect new structure

---

**This refactoring transforms a tangled codebase into a clean, maintainable architecture that supports long-term growth and development velocity.**