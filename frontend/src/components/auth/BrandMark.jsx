import React from 'react';

export const BrandMark = ({ size = 72, stroke = '#0b1b34', accent = '#d4af37', strokeWidth = 2.6, mode = 'color' }) => {

  const isMono = mode === 'mono';
  const markStroke = isMono ? '#10223f' : stroke;
  const markAccent = isMono ? '#c9d2df' : accent;
  const panFill = isMono ? '#eef2f7' : '#f3e3b1';
  const sealFill = isMono ? '#f8fafc' : '#ffffff';

  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 64 64"
      fill="none"
      aria-label="AI Lawyer brand mark"
    >
      {/* top finial */}
      <circle cx="32" cy="8" r="2" fill={markAccent} />

      {/* pillar and beam */}
      <path d="M32 11V44" stroke={markStroke} strokeWidth={strokeWidth} strokeLinecap="round" />
      <path d="M15 19H49" stroke={markStroke} strokeWidth={strokeWidth - 0.1} strokeLinecap="round" />

      {/* cords */}
      <path d="M21 19L17 27" stroke={markAccent} strokeWidth={strokeWidth - 0.7} strokeLinecap="round" />
      <path d="M25 19L29 27" stroke={markAccent} strokeWidth={strokeWidth - 0.7} strokeLinecap="round" />
      <path d="M39 19L35 27" stroke={markAccent} strokeWidth={strokeWidth - 0.7} strokeLinecap="round" />
      <path d="M43 19L47 27" stroke={markAccent} strokeWidth={strokeWidth - 0.7} strokeLinecap="round" />

      {/* judgment pans */}
      <path d="M10 29H30C28.8 34.4 24.7 38 20 38C15.3 38 11.2 34.4 10 29Z" fill={panFill} stroke={markStroke} strokeWidth={strokeWidth - 0.3} strokeLinejoin="round" />
      <path d="M34 29H54C52.8 34.4 48.7 38 44 38C39.3 38 35.2 34.4 34 29Z" fill={panFill} stroke={markStroke} strokeWidth={strokeWidth - 0.3} strokeLinejoin="round" />

      {/* center verdict seal */}
      <circle cx="32" cy="28" r="5.8" fill={sealFill} stroke={markStroke} strokeWidth={strokeWidth - 0.4} />
      <path d="M29.9 28L31.4 29.6L34.4 26.7" stroke={markAccent} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

      {/* pedestal */}
      <path d="M29 44H35V48.3H29Z" fill={markStroke} />
      <path d="M25 48.3H39V52.4H25Z" fill={markStroke} />
      <path d="M21 55H43" stroke={markStroke} strokeWidth={strokeWidth} strokeLinecap="round" />
    </svg>
  );
};
