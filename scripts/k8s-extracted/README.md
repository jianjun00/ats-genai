# K8s Extracted Scripts Directory

This directory contains all scripts that were previously embedded in Kubernetes YAML files. Following GitOps best practices, these scripts have been extracted to enable independent testing, version control, and maintenance.

## 📁 Directory Organization

```
scripts/k8s-extracted/
├── README.md                     # This file
├── app.py                        # Web application entry points
├── environment.py                # Environment configuration utilities
├── migration.sql                 # Database migration scripts
├── *_training_*.py              # ML model training scripts
├── *_backfill_*.py              # Data backfill operations
├── *_monitoring_*.py            # Monitoring and alerting logic
├── streaming_collector.py       # Real-time data collection
├── realtime_batch_validator.py  # Data validation processes
└── webapp.py                    # Web UI application logic
```

## 🎯 Purpose and Benefits

### Before: Embedded Code Issues
- ❌ 20,000+ lines of Python/shell code embedded in YAML files
- ❌ Difficult to test and debug application logic
- ❌ Poor separation of concerns (config mixed with code)
- ❌ Version control challenges for code changes
- ❌ No independent unit testing capability

### After: Extracted Script Advantages
- ✅ Clean separation between K8s config and application code
- ✅ Independent unit testing for each script
- ✅ Better version control and code review processes
- ✅ Improved maintainability and debugging
- ✅ Compliance with GitOps best practices

## 🧪 Testing Scripts

### Unit Testing
```bash
# Test all extracted scripts
python -m pytest scripts/k8s-extracted/ -v

# Test specific script types
python -m pytest scripts/k8s-extracted/environment.py -v
python -m pytest scripts/k8s-extracted/*training*.py -v

# Test with coverage
python -m pytest scripts/k8s-extracted/ --cov=scripts/k8s-extracted --cov-report=html
```

### Individual Script Testing
```bash
# Test environment configuration
PYTHONPATH=src python scripts/k8s-extracted/environment.py

# Validate training scripts
PYTHONPATH=src python scripts/k8s-extracted/train_production_model_2020_2023.py --dry-run

# Test web applications locally
python scripts/k8s-extracted/webapp.py --test-mode
```

### Integration Testing
```bash
# Test script integration with K8s
./scripts/validate_deployment.sh k8s/your-service.yaml

# Verify script references in YAML files
./scripts/detect_k8s_conflicts.py k8s/
```

## 🔧 Development Workflow

### Making Script Changes
1. **Edit Scripts**: Modify files in `scripts/k8s-extracted/`
2. **Unit Test**: Run tests to verify script functionality
3. **Integration Test**: Test within K8s environment
4. **Deploy**: Use GitOps workflow to deploy changes

```bash
# Example workflow
vim scripts/k8s-extracted/app.py                    # Make changes
python -m pytest scripts/k8s-extracted/app.py -v   # Test changes
./scripts/dev_deploy.sh                             # Deploy to K8s
```

### Adding New Scripts
1. Create script in `scripts/k8s-extracted/`
2. Make script executable: `chmod +x script_name.py`
3. Add unit tests for the script
4. Update K8s YAML to reference the new script
5. Test deployment in development environment

## 📋 Script Categories

### Application Logic (`app.py`, `webapp.py`)
- Web application entry points and UI logic
- FastAPI/Flask applications for data visualization
- Dashboard and API endpoint implementations

### Environment Configuration (`environment.py`)
- Database connection configuration
- Environment variable management  
- Service discovery and configuration utilities

### Data Processing (`*_backfill_*.py`, `streaming_collector.py`)
- Large-scale data backfill operations
- Real-time data collection and streaming
- Data transformation and ETL processes

### Machine Learning (`*_training_*.py`)
- Model training and evaluation scripts
- Feature engineering and data preparation
- Model deployment and serving logic

### Database Operations (`migration.sql`, `*_migration_*.py`)
- Database schema migrations
- Data migration and transformation scripts
- Database maintenance and optimization

### Monitoring (`*_monitoring_*.py`, `realtime_batch_validator.py`)
- System health monitoring and alerting
- Data quality validation and testing
- Performance monitoring and metrics collection

## 🚨 Important Guidelines

### Script Requirements
- **Executable**: All scripts must be executable (`chmod +x`)
- **Error Handling**: Implement proper error handling and logging
- **Environment Variables**: Use environment variables for configuration
- **Testing**: Include unit tests for all non-trivial logic
- **Documentation**: Add docstrings and comments for complex logic

### K8s Integration
- **Volume Mounts**: Scripts are mounted as volumes in K8s pods
- **Path References**: YAML files reference scripts by relative path
- **Environment Context**: Scripts run within K8s container environment
- **Resource Limits**: Consider memory and CPU requirements

### Security Considerations
- **No Secrets**: Never hardcode secrets or credentials in scripts
- **Input Validation**: Validate all external inputs and parameters
- **Least Privilege**: Scripts should request minimal required permissions
- **Audit Trail**: Log important operations for security auditing

## 📖 Related Documentation

- [Unified Development Workflow](../docs/development/UNIFIED_DEVELOPMENT_WORKFLOW.md)
- [GitOps Development Workflow](../docs/development/GITOPS_DEVELOPMENT_WORKFLOW.md)
- [CI/CD Guide](../docs/development/UNIFIED_CICD_GUIDE.md)
- [Main Scripts Directory](../scripts/README.md)

## 🛠️ Troubleshooting

### Common Issues
1. **Script Not Found**: Ensure script exists and is executable
2. **Permission Denied**: Run `chmod +x script_name.py`
3. **Import Errors**: Check PYTHONPATH and dependency installations
4. **Environment Issues**: Verify environment variables are set correctly

### Debug Commands
```bash
# Check script permissions
ls -la scripts/k8s-extracted/

# Test script execution
python scripts/k8s-extracted/environment.py

# Validate YAML references
grep -r "scripts/" k8s/ | grep -v ".yaml~"

# Check K8s deployments
kubectl get pods -n ats-dev
kubectl logs deployment/your-service -n ats-dev
```

---

*This directory was created during the K8s YAML simplification project to improve code maintainability and follow GitOps best practices.*