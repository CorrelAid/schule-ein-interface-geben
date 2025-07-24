import pyarrow as pa
from typing import Any, Dict, List, ClassVar, Optional, Type
import datetime
from pydantic import BaseModel, create_model
from polars.datatypes import Enum as PlEnum
from polars.datatypes import List as PlList
import polars as pl

# pydantic is not very compatible with dlt (no nested types, e.g. lists get converted to json strings), pyarrow and polars
# Polars schemas do not provice otion to specify if a field is nullable or not: https://github.com/pola-rs/polars/issues/16090
# Pyarrow schema do not support enums
# Cause we want both for validation (at different stages), we need to convert base schema info to both


class BaseSchema:
    """
    Base class for schema definitions.
    Subclasses must define a `fields` class variable:
      List of dicts with keys: "name", "type", "nullable".
    """

    fields: ClassVar[List[Dict[str, Any]]] = []

    @classmethod
    def to_polars_schema(cls) -> pl.Schema:
        mapping = {fld["name"]: fld["type"] for fld in cls.fields}
        return pl.Schema(mapping)

    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema:
        pa_fields = []
        for fld in cls.fields:
            name = fld["name"]
            ty = fld["type"]
            nullable = fld["nullable"]
            arrow_type = cls._pl_type_to_pa(ty)
            pa_fields.append(pa.field(name, arrow_type, nullable=nullable))
        return pa.schema(pa_fields)

    @staticmethod
    def _pl_type_to_pa(ty: Any) -> pa.DataType:
        # 1) Primitives
        if ty is pl.Int8:
            return pa.int8()
        if ty is pl.Int16:
            return pa.int16()
        if ty is pl.Int32:
            return pa.int32()
        if ty is pl.Int64:
            return pa.int64()
        if ty is pl.UInt8:
            return pa.uint8()
        if ty is pl.UInt16:
            return pa.uint16()
        if ty is pl.UInt32:
            return pa.uint32()
        if ty is pl.UInt64:
            return pa.uint64()
        if ty is pl.Float32:
            return pa.float32()
        if ty is pl.Float64:
            return pa.float64()
        if ty is pl.Boolean:
            return pa.bool_()
        if ty is pl.Utf8:
            return pa.string()
        if ty is pl.Date:
            return pa.date32()
        if ty is pl.Datetime:
            return pa.timestamp("us")
        # 2) Enums → dictionary
        if isinstance(ty, PlEnum):
            # We default to uint32 indices for all enums.
            return pa.dictionary(index_type=pa.uint32(), value_type=pa.string())
        # 3) Lists → list_<inner Arrow type>
        if isinstance(ty, PlList):
            inner = BaseSchema._pl_type_to_pa(ty.inner)
            return pa.list_(inner)
        # 4) Fallback
        raise NotImplementedError(f"Cannot map Polars type {ty} to PyArrow")

    @classmethod
    def to_pydantic_model(cls, model_name: Optional[str] = None) -> Type[BaseModel]:
        """
        Dynamically build and return a Pydantic BaseModel subclass
        that matches this schema. Use `.parse_obj(...)` to validate.
        """
        # 1) Determine model class name
        model_name = model_name or f"{cls.__name__}Model"

        # 2) Build the namespace for create_model
        annotations: Dict[str, Any] = {}
        defaults: Dict[str, Any] = {}

        for fld in cls.fields:
            name = fld["name"]
            pl_type = fld["type"]
            nullable = fld["nullable"]

            # map Polars dtype → Python type
            py_type = cls._pl_type_to_py_type_for_pydantic(pl_type)

            # wrap in Optional if it's nullable
            if nullable:
                py_type = Optional[py_type]
                default = None
            else:
                default = ...

            annotations[name] = py_type
            defaults[name] = default

        # 3) Create the model
        model = create_model(
            model_name,
            __config__=None,  # or custom Config class
            **{k: (annotations[k], defaults[k]) for k in annotations},
        )

        return model

    @staticmethod
    def _pl_type_to_py_type_for_pydantic(ty: Any) -> Any:
        from polars.datatypes import (
            Int8,
            Int16,
            Int32,
            Int64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Float32,
            Float64,
            Boolean,
        )

        # 1) Explicit numeric + bool mappings
        if ty in (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64):
            return int
        if ty in (Float32, Float64):
            return float
        if ty is Boolean:
            return bool

        # 2) List[...] → List[inner_type]
        if isinstance(ty, pl.datatypes.List):
            inner = ty.inner
            inner_py = BaseSchema._pl_type_to_py_type_for_pydantic(inner)
            return List[inner_py]

        # 3) Dates & times
        if ty is pl.Date:
            return datetime.date
        if ty in (pl.Datetime, pl.Time, pl.Duration):
            return datetime.datetime

        # 4) Fallback on NumPy‐convertible types
        if hasattr(ty, "to_python"):
            npdt = ty.to_python()
            kind = getattr(npdt, "kind", None)
            if kind == "M":  # datetime64
                return datetime.datetime

        # 5) All else → str
        return str
