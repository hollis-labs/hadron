import { Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface KVEditorProps {
  data: Record<string, string>;
  onChange: (data: Record<string, string>) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
}

export function KVEditor({ data, onChange, keyPlaceholder, valuePlaceholder }: KVEditorProps) {
  const entries = Object.entries(data);
  const updateKey = (oldKey: string, newKey: string, value: string) => {
    const next = { ...data };
    delete next[oldKey];
    next[newKey] = value;
    onChange(next);
  };
  const updateValue = (key: string, value: string) => {
    onChange({ ...data, [key]: value });
  };
  const remove = (key: string) => {
    const next = { ...data };
    delete next[key];
    onChange(next);
  };
  const add = () => {
    onChange({ ...data, '': '' });
  };

  return (
    <div>
      {entries.map(([key, value], i) => (
        <div key={i} className="grid grid-cols-[1fr_1fr_auto] gap-2 mb-1.5 items-center">
          <Input value={key} placeholder={keyPlaceholder || 'key'} onChange={e => updateKey(key, e.target.value, value)} />
          <Input value={value} placeholder={valuePlaceholder || 'value'} onChange={e => updateValue(key, e.target.value)} />
          <Button variant="ghost" size="icon-sm" onClick={() => remove(key)}><Trash2 size={13} className="text-red-400" /></Button>
        </div>
      ))}
      <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={add}><Plus size={14} /> Add</button>
    </div>
  );
}
