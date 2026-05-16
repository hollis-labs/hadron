import { useState, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface CronBuilderProps {
  value: string;
  onChange: (cron: string) => void;
}

interface CronFields {
  minute: string;
  hour: string;
  dayOfMonth: string;
  month: string;
  dayOfWeek: string;
}

const FIELD_META: { key: keyof CronFields; label: string; placeholder: string; hint: string; range: string }[] = [
  { key: 'minute',     label: 'Min',   placeholder: '*',  hint: '0–59',  range: '0-59' },
  { key: 'hour',       label: 'Hour',  placeholder: '*',  hint: '0–23',  range: '0-23' },
  { key: 'dayOfMonth', label: 'Day',   placeholder: '*',  hint: '1–31',  range: '1-31' },
  { key: 'month',      label: 'Month', placeholder: '*',  hint: '1–12',  range: '1-12' },
  { key: 'dayOfWeek',  label: 'Wkday', placeholder: '*',  hint: '0–6',   range: '0-6' },
];

// Presets for quick selection
const PRESETS = [
  { label: 'Every minute',     cron: '* * * * *' },
  { label: 'Every 5 min',      cron: '*/5 * * * *' },
  { label: 'Every 15 min',     cron: '*/15 * * * *' },
  { label: 'Hourly',           cron: '0 * * * *' },
  { label: 'Daily at midnight', cron: '0 0 * * *' },
  { label: 'Daily at 2 AM',    cron: '0 2 * * *' },
  { label: 'Weekly (Sun)',     cron: '0 0 * * 0' },
  { label: 'Monthly (1st)',    cron: '0 0 1 * *' },
];

// Validate a single cron field token
function validateField(value: string, rangeStr: string): string | null {
  if (!value || value === '*') return null;

  // Allow step syntax: */N or N-M/N
  const stepPattern = /^(\*|\d+(-\d+)?)\/\d+$/;
  // Allow range: N-M
  const rangePattern = /^\d+-\d+$/;
  // Allow list: N,M,...
  const listPattern = /^\d+(,\d+)*$/;
  // Simple number
  const numberPattern = /^\d+$/;

  if (stepPattern.test(value) || rangePattern.test(value) || listPattern.test(value) || numberPattern.test(value)) {
    // Check all numbers are within range
    const [lo, hi] = rangeStr.split('-').map(Number);
    const nums = value.replace(/[*/]/g, ',').split(',').filter(s => /^\d+$/.test(s)).map(Number);
    for (const n of nums) {
      if (n < lo || n > hi) return `Out of range (${rangeStr})`;
    }
    return null;
  }
  return 'Invalid syntax';
}

function parseCron(cron: string): CronFields {
  const parts = cron.trim().split(/\s+/);
  return {
    minute:     parts[0] ?? '*',
    hour:       parts[1] ?? '*',
    dayOfMonth: parts[2] ?? '*',
    month:      parts[3] ?? '*',
    dayOfWeek:  parts[4] ?? '*',
  };
}

function assembleCron(fields: CronFields): string {
  return `${fields.minute} ${fields.hour} ${fields.dayOfMonth} ${fields.month} ${fields.dayOfWeek}`;
}

// Human-readable description of the cron expression
function describeCron(fields: CronFields): string {
  const { minute, hour, dayOfMonth, month, dayOfWeek } = fields;

  const parts: string[] = [];

  // Minute
  if (minute === '*') parts.push('every minute');
  else if (minute.startsWith('*/')) parts.push(`every ${minute.slice(2)} min`);
  else parts.push(`at minute ${minute}`);

  // Hour
  if (hour === '*') { /* every hour, skip */ }
  else if (hour.startsWith('*/')) parts.push(`every ${hour.slice(2)}h`);
  else {
    const h = parseInt(hour);
    if (!isNaN(h)) {
      const ampm = h >= 12 ? 'PM' : 'AM';
      const h12 = h === 0 ? 12 : h > 12 ? h - 12 : h;
      parts.push(`at ${h12} ${ampm}`);
    } else {
      parts.push(`hour ${hour}`);
    }
  }

  // Day of month
  if (dayOfMonth !== '*') parts.push(`on day ${dayOfMonth}`);

  // Month
  if (month !== '*') {
    const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const m = parseInt(month);
    parts.push(`in ${monthNames[m] ?? `month ${month}`}`);
  }

  // Day of week
  if (dayOfWeek !== '*') {
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const d = parseInt(dayOfWeek);
    parts.push(`on ${dayNames[d] ?? `weekday ${dayOfWeek}`}`);
  }

  return parts.join(', ');
}

export function CronBuilder({ value, onChange }: CronBuilderProps) {
  const [fields, setFields] = useState<CronFields>(parseCron(value || '* * * * *'));
  const [errors, setErrors] = useState<Record<string, string | null>>({});
  const [showPresets, setShowPresets] = useState(false);

  // Sync external value changes into fields
  useEffect(() => {
    if (value) {
      const parsed = parseCron(value);
      setFields(parsed);
    }
  }, [value]);

  const handleFieldChange = useCallback((key: keyof CronFields, val: string) => {
    // Only allow valid cron characters
    const cleaned = val.replace(/[^0-9*,-/]/g, '');
    const updated = { ...fields, [key]: cleaned };
    setFields(updated);

    // Validate
    const meta = FIELD_META.find(m => m.key === key)!;
    const err = validateField(cleaned, meta.range);
    setErrors(prev => ({ ...prev, [key]: err }));

    // Only fire onChange if all fields are valid
    const allErrors = { ...errors, [key]: err };
    const hasErrors = Object.values(allErrors).some(e => e !== null && e !== undefined);
    if (!hasErrors) {
      onChange(assembleCron(updated));
    }
  }, [fields, errors, onChange]);

  const handlePreset = (cron: string) => {
    const parsed = parseCron(cron);
    setFields(parsed);
    setErrors({});
    onChange(cron);
    setShowPresets(false);
  };

  const assembled = assembleCron(fields);
  const description = describeCron(fields);
  const hasAnyError = Object.values(errors).some(e => e !== null && e !== undefined);

  return (
    <div>
      {/* Preset selector */}
      <div style={{ marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
        <Button
          type="button"
          variant="ghost"
          size="xs"
          onClick={() => setShowPresets(!showPresets)}
        >
          Presets {showPresets ? '▴' : '▾'}
        </Button>
        {showPresets && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.25rem' }}>
            {PRESETS.map(p => (
              <Button
                key={p.cron}
                type="button"
                variant={assembled === p.cron ? 'outline' : 'ghost'}
                size="xs"
                onClick={() => handlePreset(p.cron)}
              >
                {p.label}
              </Button>
            ))}
          </div>
        )}
      </div>

      {/* Fields row */}
      <div style={{ display: 'flex', gap: '0.35rem', alignItems: 'flex-start' }}>
        {FIELD_META.map(meta => {
          const fieldErr = errors[meta.key];
          return (
            <div key={meta.key} style={{ flex: 1, minWidth: 0 }}>
              <Input
                type="text"
                value={fields[meta.key]}
                onChange={e => handleFieldChange(meta.key, e.target.value)}
                placeholder={meta.placeholder}
                style={{
                  width: '100%',
                  boxSizing: 'border-box',
                  fontFamily: 'monospace',
                  fontSize: 'var(--text-base)',
                  textAlign: 'center',
                  padding: '0.4rem 0.25rem',
                  ...(fieldErr ? { borderColor: 'rgb(var(--danger))' } : {}),
                }}
              />
              <div style={{
                fontSize: 'var(--text-xs)',
                textAlign: 'center',
                marginTop: '0.2rem',
                letterSpacing: '0.04em',
                color: fieldErr ? 'rgb(var(--danger))' : 'rgb(var(--muted))',
                fontWeight: 600,
                textTransform: 'uppercase',
              }}>
                {meta.label}
              </div>
              <div style={{
                fontSize: 'var(--text-xs)',
                textAlign: 'center',
                color: 'rgb(var(--muted))',
                opacity: 0.7,
              }}>
                {meta.hint}
              </div>
            </div>
          );
        })}
      </div>

      {/* Assembled expression + description */}
      <div style={{
        marginTop: '0.5rem',
        padding: '0.35rem 0.6rem',
        background: 'rgb(var(--panel2))',
        borderRadius: '4px',
        border: `1px solid ${hasAnyError ? 'rgba(var(--danger) / 0.4)' : 'rgb(var(--border))'}`,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: '0.5rem',
      }}>
        <span style={{
          fontFamily: 'monospace',
          fontSize: 'var(--text-sm)',
          color: hasAnyError ? 'rgb(var(--danger))' : 'rgb(var(--accent))',
          fontWeight: 600,
          whiteSpace: 'nowrap',
        }}>
          {assembled}
        </span>
        <span style={{
          fontSize: 'var(--text-xs)',
          color: 'rgb(var(--muted))',
          textAlign: 'right',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}>
          {hasAnyError ? 'Fix errors above' : description}
        </span>
      </div>
    </div>
  );
}
