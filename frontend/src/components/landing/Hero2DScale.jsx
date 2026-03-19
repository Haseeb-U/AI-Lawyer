import React from 'react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';

export default function Hero2DScale() {
  // Gentle, slow see-saw motion for the main structural beam
  const beamVariants = {
    animate: { 
      rotate: [-5, 5, -5], 
      transition: { duration: 7, ease: "easeInOut", repeat: Infinity } 
    }
  };

  // Exact counter-rotation so the pans stay perfectly horizontal 
  // with respect to gravity as they translate up and down.
  const panVariants = {
    animate: { 
      rotate: [5, -5, 5], 
      transition: { duration: 7, ease: "easeInOut", repeat: Infinity } 
    }
  };

  return (
    <div className="relative w-full max-w-[550px] aspect-square flex items-center justify-center scale-110 md:scale-125 lg:scale-[1.3] origin-center translate-y-6">
      
      {/* Background ambient glow matching professional theme */}
      <div className="absolute inset-0 bg-gradient-to-tr from-[#d4af37]/20 via-[#051326]/5 to-[#051326]/10 rounded-full blur-[80px] z-0" />

      <svg 
        viewBox="0 0 500 500" 
        className="w-full h-full relative z-10 drop-shadow-[0_20px_30px_rgba(0,0,0,0.3)]"
        style={{ overflow: "visible" }}
      >
        <defs>
          <linearGradient id="goldHorizontal" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#bf953f" />
            <stop offset="30%" stopColor="#fcf6ba" />
            <stop offset="50%" stopColor="#d4af37" />
            <stop offset="80%" stopColor="#fcf6ba" />
            <stop offset="100%" stopColor="#bf953f" />
          </linearGradient>

          <linearGradient id="goldVertical" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="#fcf6ba" />
            <stop offset="40%" stopColor="#d4af37" />
            <stop offset="100%" stopColor="#8a6d1c" />
          </linearGradient>

          <linearGradient id="navyVertical" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="#0a2347" />
            <stop offset="50%" stopColor="#051326" />
            <stop offset="100%" stopColor="#02070e" />
          </linearGradient>

          <linearGradient id="navyHorizontal" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#040e1c" />
            <stop offset="50%" stopColor="#0a2347" />
            <stop offset="100%" stopColor="#040e1c" />
          </linearGradient>
        </defs>

        {/* === STATIC PILLAR & BASE === */}
        <g>
          {/* Pillar Shaft */}
          <rect x="238" y="140" width="24" height="240" fill="url(#navyHorizontal)" />
          
          {/* Pillar Gold Accent Rings */}
          <rect x="236" y="170" width="28" height="6" fill="url(#goldHorizontal)" rx="2" />
          <rect x="236" y="340" width="28" height="6" fill="url(#goldHorizontal)" rx="2" />
          <rect x="234" y="370" width="32" height="10" fill="url(#goldHorizontal)" rx="2" />

          {/* Base Layer 1 (Top) */}
          <path d="M200 380 L300 380 L310 405 L190 405 Z" fill="url(#navyVertical)" />
          {/* Base Layer 2 (Middle) */}
          <path d="M170 405 L330 405 L340 435 L160 435 Z" fill="#051326" stroke="url(#goldHorizontal)" strokeWidth="1" />
          {/* Gold Trim inside middle base */}
          <line x1="180" y1="415" x2="320" y2="415" stroke="url(#goldHorizontal)" strokeWidth="2" opacity="0.6" />
          <line x1="172" y1="425" x2="328" y2="425" stroke="url(#goldHorizontal)" strokeWidth="1" opacity="0.4" />
          {/* Base Layer 3 (Bottom) */}
          <path d="M140 435 L360 435 L360 460 L140 460 Z" fill="url(#navyVertical)" />
          
          {/* Top Cap (Spire under the pivot) */}
          <path d="M236 140 L264 140 L250 110 Z" fill="url(#goldHorizontal)" />
          <circle cx="250" cy="110" r="10" fill="url(#goldVertical)" />
        </g>

        {/* === ROTATING BEAM & HOOKS === */}
        <motion.g variants={beamVariants} animate="animate" style={{ transformOrigin: '250px 150px' }}>
          
          {/* Main Balance Bar */}
          <rect x="70" y="144" width="360" height="12" rx="6" fill="url(#goldHorizontal)" />
          
          {/* Left Hook Mount */}
          <circle cx="90" cy="150" r="10" fill="url(#navyHorizontal)" stroke="url(#goldVertical)" strokeWidth="2" />
          
          {/* Right Hook Mount */}
          <circle cx="410" cy="150" r="10" fill="url(#navyHorizontal)" stroke="url(#goldVertical)" strokeWidth="2" />

          {/* Center Pivot Joint */}
          <circle cx="250" cy="150" r="18" fill="url(#navyHorizontal)" stroke="url(#goldVertical)" strokeWidth="3" />
          <circle cx="250" cy="150" r="6" fill="url(#goldVertical)" />

          {/* === LEFT PAN ASSEMBLY === */}
          <motion.g variants={panVariants} animate="animate" style={{ transformOrigin: '90px 150px' }}>
            {/* Hook Ring */}
            <circle cx="90" cy="158" r="6" fill="transparent" stroke="url(#goldVertical)" strokeWidth="2" />
            
            {/* Strings (3 strings down to the bowl) */}
            <line x1="90" y1="164" x2="90" y2="305" stroke="#a38221" strokeWidth="1.5" />
            <line x1="88" y1="164" x2="35" y2="310" stroke="url(#goldVertical)" strokeWidth="2.5" />
            <line x1="92" y1="164" x2="145" y2="310" stroke="url(#goldVertical)" strokeWidth="2.5" />

            {/* Scale Plate / Bowl */}
            <path d="M 35 310 C 35 345, 145 345, 145 310 Z" fill="url(#goldVertical)" />
            {/* Inner Pan Surface */}
            <ellipse cx="90" cy="310" rx="55" ry="14" fill="#e6ca6a" stroke="#ffffff" strokeOpacity="0.4" strokeWidth="1.5" />
            <ellipse cx="90" cy="310" rx="45" ry="9" fill="transparent" stroke="#a38221" strokeOpacity="0.3" strokeWidth="1" />
          </motion.g>

          {/* === RIGHT PAN ASSEMBLY === */}
          <motion.g variants={panVariants} animate="animate" style={{ transformOrigin: '410px 150px' }}>
            {/* Hook Ring */}
            <circle cx="410" cy="158" r="6" fill="transparent" stroke="url(#goldVertical)" strokeWidth="2" />
            
            {/* Strings */}
            <line x1="410" y1="164" x2="410" y2="305" stroke="#a38221" strokeWidth="1.5" />
            <line x1="408" y1="164" x2="355" y2="310" stroke="url(#goldVertical)" strokeWidth="2.5" />
            <line x1="412" y1="164" x2="465" y2="310" stroke="url(#goldVertical)" strokeWidth="2.5" />

            {/* Scale Plate / Bowl */}
            <path d="M 355 310 C 355 345, 465 345, 465 310 Z" fill="url(#goldVertical)" />
            {/* Inner Pan Surface */}
            <ellipse cx="410" cy="310" rx="55" ry="14" fill="#e6ca6a" stroke="#ffffff" strokeOpacity="0.4" strokeWidth="1.5" />
            <ellipse cx="410" cy="310" rx="45" ry="9" fill="transparent" stroke="#a38221" strokeOpacity="0.3" strokeWidth="1" />
          </motion.g>

        </motion.g>
      </svg>
    </div>
  );
}
