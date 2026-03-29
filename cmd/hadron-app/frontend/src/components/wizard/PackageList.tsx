import { useState } from 'react';
import { Input } from '@/components/ui/input';

interface PackageListProps {
  items: string[];
  onAdd: (v: string) => void;
  onRemove: (i: number) => void;
  placeholder: string;
}

export function PackageList({ items, onAdd, onRemove, placeholder }: PackageListProps) {
  const [val, setVal] = useState('');
  return (
    <div>
      <div className="flex flex-wrap gap-1 mb-1.5">
        {items.map((pkg, i) => (
          <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-muted text-muted-foreground mr-1 mb-1">
            {pkg}
            <button onClick={() => onRemove(i)} className="ml-1 cursor-pointer bg-transparent border-none text-inherit text-xs">&times;</button>
          </span>
        ))}
      </div>
      <Input value={val} placeholder={placeholder} className="w-full"
        onKeyDown={e => { if (e.key === 'Enter' && val.trim()) { onAdd(val.trim()); setVal(''); e.preventDefault(); } }}
        onChange={e => setVal(e.target.value)} />
    </div>
  );
}
