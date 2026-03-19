import React from 'react';
import { Input } from '../ui/input';

export const AuthInput = ({ label, name, type = 'text', placeholder, value, onChange, required }) => (
    <div className="mb-5 relative">
        <label className="block text-[13px] font-bold text-[var(--foreground)] mb-1.5">{label}</label>
        <div className="relative">
            <Input 
                type={type} 
                name={name} 
                placeholder={placeholder} 
                value={value} 
                onChange={onChange} 
                required={required} 
                className="w-full bg-[var(--background)] border-[var(--card-border)] text-[var(--foreground)] placeholder:text-[var(--text-muted)] focus-visible:ring-[#d4af37] focus-visible:border-[#d4af37] text-sm h-11"
            />
            {value?.length > 0 && (
                <svg className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-emerald-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" d="M5 13l4 4L19 7"/>
                </svg>
            )}
        </div>
    </div>
);
