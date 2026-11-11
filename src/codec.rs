/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

//! The conversion between the physical value of an item (a register, a node, a tag) and the logical value of its
//! parameter: range mapping, scale and offset, rounding, inverted indicators, and the bounds of the parameter.

use crate::model::*;
use rust_decimal::{Decimal, RoundingStrategy};

#[derive(Clone, Debug)]
pub struct ValueCodec {
    pub physical_min: Option<Decimal>,
    pub physical_max: Option<Decimal>,
    pub logical_min: Option<Decimal>,
    pub logical_max: Option<Decimal>,
    pub scale: Decimal,
    pub offset: Decimal,
    pub digits: Option<u32>,
    pub tolerance: Decimal,
    pub reverse_logic: bool,
    pub allow_duplicate: bool,
}

impl Default for ValueCodec {
    fn default() -> Self {
        Self {
            physical_min: None,
            physical_max: None,
            logical_min: None,
            logical_max: None,
            scale: Decimal::ONE,
            offset: Decimal::ZERO,
            digits: None,
            tolerance: Decimal::ZERO,
            reverse_logic: false,
            allow_duplicate: false,
        }
    }
}

fn checked(v: Option<Decimal>) -> Result<Decimal, DeviceGatewayError> {
    v.ok_or_else(|| DeviceGatewayError::invalid("Numeric conversion overflow"))
}

fn map(v: Decimal, a: Decimal, b: Decimal, c: Decimal, d: Decimal) -> Result<Decimal, DeviceGatewayError> {
    checked(checked(v.checked_sub(a))?.checked_mul(checked(d.checked_sub(c))?))?
        .checked_div(checked(b.checked_sub(a))?)
        .and_then(|x| x.checked_add(c))
        .ok_or_else(|| DeviceGatewayError::invalid("Invalid numeric mapping range"))
}

impl ValueCodec {
    pub fn validate(&self) -> Result<(), DeviceGatewayError> {
        if self.scale.is_zero() || self.tolerance < Decimal::ZERO || self.digits.is_some_and(|d| d > 28) {
            return Err(DeviceGatewayError::invalid(
                "Invalid scale, tolerance or decimal precision",
            ));
        }
        if self.physical_min.zip(self.physical_max).is_some_and(|(a, b)| a >= b)
            || self.logical_min.zip(self.logical_max).is_some_and(|(a, b)| a >= b)
        {
            return Err(DeviceGatewayError::invalid("Invalid minimum/maximum range"));
        }
        Ok(())
    }

    /// A value read is limited to the bounds of its parameter.
    fn clamp(&self, mut n: Decimal) -> Decimal {
        if let Some(v) = self.logical_min {
            n = n.max(v);
        }
        if let Some(v) = self.logical_max {
            n = n.min(v);
        }
        n
    }

    fn round(&self, n: Decimal) -> Decimal {
        self.digits.map_or(n, |d| {
            n.round_dp_with_strategy(d, RoundingStrategy::MidpointAwayFromZero)
        })
    }

    pub fn decode(&self, value: &FieldValue, kind: ParameterKind) -> Result<FieldValue, DeviceGatewayError> {
        self.validate()?;
        match kind {
            ParameterKind::Numeric => {
                let mut n = value.decimal()?;
                if let (Some(a), Some(b), Some(c), Some(d)) =
                    (self.physical_min, self.physical_max, self.logical_min, self.logical_max)
                {
                    n = map(n, a, b, c, d)?;
                }
                n = checked(checked(n.checked_mul(self.scale))?.checked_add(self.offset))?;
                Ok(FieldValue::Decimal(self.round(self.clamp(n))))
            }
            ParameterKind::Indicator => match value {
                FieldValue::Bool(v) => Ok(FieldValue::Bool(*v ^ self.reverse_logic)),
                FieldValue::Text(v) if v == "Y" || v == "N" => Ok(FieldValue::Bool((v == "Y") ^ self.reverse_logic)),
                _ => Err(DeviceGatewayError::invalid("Indicator requires BOOL or Y/N")),
            },
            ParameterKind::Bits => match value {
                FieldValue::Unsigned(v) => Ok(FieldValue::Unsigned(*v)),
                FieldValue::Signed(v) if *v >= 0 => Ok(FieldValue::Unsigned(*v as u64)),
                FieldValue::Text(s) => {
                    let s = s.strip_prefix("0b").unwrap_or(s).replace([' ', '_'], "");
                    Ok(FieldValue::Unsigned(
                        u64::from_str_radix(&s, 2).map_err(|_| DeviceGatewayError::invalid("Invalid binary string"))?,
                    ))
                }
                _ => Err(DeviceGatewayError::invalid("Bitset requires an unsigned value")),
            },
            ParameterKind::Text => Ok(value.clone()),
        }
    }

    pub fn encode(
        &self,
        value: &FieldValue,
        kind: ParameterKind,
        ty: PhysicalType,
    ) -> Result<FieldValue, DeviceGatewayError> {
        self.validate()?;
        if kind == ParameterKind::Indicator {
            return match value {
                FieldValue::Bool(v) => Ok(FieldValue::Bool(*v ^ self.reverse_logic)),
                _ => Err(DeviceGatewayError::invalid(
                    "Indicator requires a boolean logical value",
                )),
            };
        }
        if kind == ParameterKind::Text {
            return Ok(value.clone());
        }
        let mut n = value.decimal()?;
        if kind == ParameterKind::Numeric {
            // A setpoint beyond the bounds of its parameter is refused, never limited: the machine gets the value
            // asked for or nothing.
            if self.logical_min.is_some_and(|min| n < min) || self.logical_max.is_some_and(|max| n > max) {
                let bound = |b: Option<Decimal>| b.map_or("-".to_string(), |b| b.to_string());
                return Err(DeviceGatewayError::invalid(format!(
                    "Value {n} is outside the range {}..{} of the parameter",
                    bound(self.logical_min),
                    bound(self.logical_max)
                )));
            }
            n = checked(checked(n.checked_sub(self.offset))?.checked_div(self.scale))?;
            if let (Some(a), Some(b), Some(c), Some(d)) =
                (self.logical_min, self.logical_max, self.physical_min, self.physical_max)
            {
                n = map(n, a, b, c, d)?;
            }
            n = self.round(n);
        }
        let invalid = || DeviceGatewayError::invalid("Value is outside the physical type range");
        Ok(match ty {
            PhysicalType::U8 | PhysicalType::U16 | PhysicalType::U32 | PhysicalType::U64 => {
                let max = match ty {
                    PhysicalType::U8 => u8::MAX as u64,
                    PhysicalType::U16 => u16::MAX as u64,
                    PhysicalType::U32 => u32::MAX as u64,
                    _ => u64::MAX,
                };
                if !n.fract().is_zero() {
                    return Err(invalid());
                }
                let v = u64::try_from(n).map_err(|_| invalid())?;
                if v > max {
                    return Err(invalid());
                }
                FieldValue::Unsigned(v)
            }
            PhysicalType::I8 | PhysicalType::I16 | PhysicalType::I32 | PhysicalType::I64 => {
                let (min, max) = match ty {
                    PhysicalType::I8 => (i8::MIN as i64, i8::MAX as i64),
                    PhysicalType::I16 => (i16::MIN as i64, i16::MAX as i64),
                    PhysicalType::I32 => (i32::MIN as i64, i32::MAX as i64),
                    _ => (i64::MIN, i64::MAX),
                };
                if !n.fract().is_zero() {
                    return Err(invalid());
                }
                let v = i64::try_from(n).map_err(|_| invalid())?;
                if v < min || v > max {
                    return Err(invalid());
                }
                FieldValue::Signed(v)
            }
            PhysicalType::F32 | PhysicalType::F64 => {
                let v = n.to_string().parse::<f64>().map_err(|_| invalid())?;
                if !v.is_finite() || (ty == PhysicalType::F32 && !(v as f32).is_finite()) {
                    return Err(invalid());
                }
                FieldValue::Float(if ty == PhysicalType::F32 { (v as f32) as f64 } else { v })
            }
            _ => {
                return Err(DeviceGatewayError::invalid(
                    "Numeric mapping requires a numeric physical type",
                ));
            }
        })
    }
}
