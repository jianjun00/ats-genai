#!/usr/bin/env python3
"""
Type-Aware Analytics API
Provides intelligent filter generation and schema-driven EDA capabilities.
"""

import logging
from typing import Any
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

# Import type system components
from domains.ml.schema.registry import schema_registry
from domains.ml.schema.types import FieldSemantics
from domains.analytics.services.analytics_service import AnalyticsService

logger = logging.getLogger(__name__)

# This will be integrated into the main analytics app
def create_type_aware_analytics_router():
    """Create router with type-aware analytics endpoints."""
    from fastapi import APIRouter
    router = APIRouter(prefix="/api/v1/type-aware", tags=["type-aware-analytics"])

    @router.get("/filters/{table_name}")
    async def get_intelligent_filters(
        table_name: str,
        analytics_service: AnalyticsService = Depends(get_analytics_service)
    ):
        """Get intelligent filter definitions for a table using type system."""
        try:
            logger.info(f"Getting intelligent filters for table: {table_name}")
            filters = await analytics_service.get_intelligent_filters(table_name)

            return {
                "status": "success",
                "table_name": table_name,
                "filters": filters,
                "message": f"Generated {filters['total_filterable_fields']} intelligent filters"
            }

        except Exception as e:
            logger.error(f"Error getting filters for {table_name}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/schema/{table_name}")
    async def get_table_schema_info(table_name: str):
        """Get comprehensive schema information for a table."""
        try:
            # Try to get from schema registry first
            try:
                schema = schema_registry.get_table_schema(table_name)

                # Build comprehensive schema info
                schema_info = {
                    "table_name": table_name,
                    "entity_name": schema.entity_name,
                    "description": schema.description,
                    "total_fields": len(schema.fields),
                    "primary_key": schema.primary_key,
                    "indexes": schema.indexes,
                    "type_system_enabled": True
                }

                # Field breakdown by semantics
                field_breakdown = {}
                for field_name, field_def in schema.fields.items():
                    semantic_type = field_def.semantics.value
                    if semantic_type not in field_breakdown:
                        field_breakdown[semantic_type] = []
                    field_breakdown[semantic_type].append({
                        "name": field_name,
                        "type": field_def.field_type.value,
                        "ui_label": field_def.ui_label,
                        "priority": field_def.eda_priority,
                        "nullable": field_def.nullable
                    })

                schema_info["field_breakdown"] = field_breakdown

                # UI generation hints
                ui_components = {}
                for field_name, field_def in schema.fields.items():
                    if field_def.is_filterable:
                        if field_def.semantics == FieldSemantics.SEARCHABLE_STRING:
                            component = "text_input_with_autocomplete"
                        elif field_def.semantics == FieldSemantics.CATEGORICAL:
                            component = "dropdown_with_predefined_options" if field_def.enum_values else "dropdown_with_db_query"
                        elif field_def.semantics == FieldSemantics.NUMERIC_RANGE:
                            component = "range_slider"
                        elif field_def.semantics == FieldSemantics.DATE_RANGE:
                            component = "date_range_picker"
                        elif field_def.semantics == FieldSemantics.BOOLEAN:
                            component = "tri_state_checkbox"
                        else:
                            component = "display_only"

                        ui_components[field_name] = {
                            "component": component,
                            "priority": field_def.eda_priority,
                            "requires_db_query": not bool(field_def.enum_values) if field_def.semantics == FieldSemantics.CATEGORICAL else False
                        }

                schema_info["ui_components"] = ui_components

                return {
                    "status": "success",
                    "schema": schema_info,
                    "performance_benefits": {
                        "predefined_enums": sum(1 for f in schema.fields.values() if f.enum_values),
                        "no_db_queries_needed": sum(1 for f in schema.fields.values() if f.enum_values and f.semantics == FieldSemantics.CATEGORICAL),
                        "intelligent_ui_generation": len([f for f in schema.fields.values() if f.is_filterable])
                    }
                }

            except ValueError:
                # Table not in schema registry
                return {
                    "status": "legacy",
                    "table_name": table_name,
                    "type_system_enabled": False,
                    "message": "Table not found in type system schema registry. Falling back to database inspection.",
                    "schema": None
                }

        except Exception as e:
            logger.error(f"Error getting schema info for {table_name}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/registry/summary")
    async def get_schema_registry_summary():
        """Get comprehensive summary of the schema registry."""
        try:
            summary = schema_registry.get_schema_summary()

            # Add transformation benefits
            transformation_benefits = {
                "total_entities": summary["total_entities"],
                "total_tables": summary["total_tables"],
                "total_predefined_enums": 0,
                "total_intelligent_filters": 0,
                "performance_optimizations": 0
            }

            for entity_name, entity_info in summary["entities"].items():
                schema = schema_registry.get_schema(entity_name)
                for field_name, field_def in schema.fields.items():
                    if field_def.enum_values:
                        transformation_benefits["total_predefined_enums"] += 1
                        if field_def.semantics == FieldSemantics.CATEGORICAL:
                            transformation_benefits["performance_optimizations"] += 1
                    if field_def.is_filterable:
                        transformation_benefits["total_intelligent_filters"] += 1

            return {
                "status": "success",
                "registry_summary": summary,
                "transformation_benefits": transformation_benefits,
                "system_capabilities": {
                    "intelligent_filter_generation": True,
                    "predefined_enum_values": True,
                    "semantic_field_classification": True,
                    "automatic_ui_component_selection": True,
                    "performance_optimized": True,
                    "validation_enabled": True
                }
            }

        except Exception as e:
            logger.error(f"Error getting registry summary: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/enum-values/{table_name}/{field_name}")
    async def get_enum_values(table_name: str, field_name: str):
        """Get predefined enum values for a field (performance optimized)."""
        try:
            enum_values = schema_registry.get_enum_values(table_name, field_name)

            if enum_values:
                return {
                    "status": "success",
                    "table_name": table_name,
                    "field_name": field_name,
                    "enum_values": enum_values,
                    "count": len(enum_values),
                    "performance_optimized": True,
                    "message": "Values retrieved from schema registry (no database query needed)"
                }
            else:
                return {
                    "status": "no_predefined_values",
                    "table_name": table_name,
                    "field_name": field_name,
                    "enum_values": None,
                    "performance_optimized": False,
                    "message": "Field has no predefined enum values. Database query required."
                }

        except Exception as e:
            logger.error(f"Error getting enum values for {table_name}.{field_name}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/validate/{table_name}/{field_name}")
    async def validate_field_value(
        table_name: str,
        field_name: str,
        value: Any,
        analytics_service: AnalyticsService = Depends(get_analytics_service)
    ):
        """Validate a field value using type system rules."""
        try:
            is_valid = schema_registry.validate_field_value(table_name, field_name, value)
            field_def = schema_registry.get_field_definition(table_name, field_name)

            validation_info = {
                "status": "success",
                "table_name": table_name,
                "field_name": field_name,
                "value": value,
                "is_valid": is_valid,
                "field_type": field_def.field_type.value if field_def else "unknown",
                "semantics": field_def.semantics.value if field_def else "unknown"
            }

            if field_def and not is_valid:
                validation_info["validation_rules"] = {
                    "nullable": field_def.nullable,
                    "max_length": field_def.max_length,
                    "min_value": field_def.min_value,
                    "max_value": field_def.max_value,
                    "enum_values": field_def.enum_values,
                    "validation_regex": field_def.validation_regex
                }

            return validation_info

        except Exception as e:
            logger.error(f"Error validating {table_name}.{field_name} = {value}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router


def get_analytics_service():
    """Dependency to get analytics service instance."""
    # This will be overridden by the main app through dependency injection
    from services.analytics.unified_analytics_app import service
    return service.type_aware_analytics


if __name__ == "__main__":
    # For testing - create a standalone FastAPI app
    app = FastAPI(
        title="Type-Aware Analytics API",
        description="Intelligent filter generation using ATS type system",
        version="1.0.0"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include the router
    router = create_type_aware_analytics_router()
    app.include_router(router)

    @app.get("/")
    async def root():
        return {
            "message": "Type-Aware Analytics API",
            "version": "1.0.0",
            "features": [
                "Intelligent filter generation",
                "Schema-driven EDA",
                "Performance-optimized enum handling",
                "Automatic UI component selection",
                "Field validation",
                "Legacy system compatibility"
            ],
            "endpoints": [
                "/api/v1/type-aware/filters/{table_name}",
                "/api/v1/type-aware/schema/{table_name}",
                "/api/v1/type-aware/registry/summary",
                "/api/v1/type-aware/enum-values/{table_name}/{field_name}",
                "/api/v1/type-aware/validate/{table_name}/{field_name}"
            ]
        }