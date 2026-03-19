import React, { Suspense } from 'react';
import { ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';
import { Button } from '../ui/button';

// Lazy loading heavy visual assets
const Hero2DScale = React.lazy(() => import('./Hero2DScale'));

export default function HeroSection() {
  const navigate = useNavigate();

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: { staggerChildren: 0.15 }
    }
  };

  const itemVariants = {
    hidden: { opacity: 0, y: 15 },
    visible: { opacity: 1, y: 0, transition: { duration: 0.6, ease: "easeOut" } }
  };

  return (
    <section id="home" className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[var(--background)] px-4 pt-24 pb-12 smooth-transition">
      <div className="relative z-10 max-w-7xl mx-auto w-full grid grid-cols-1 lg:grid-cols-2 gap-12 lg:gap-8 items-center">
        
        {/* Left Column: Typography & CTAs */}
        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="flex flex-col items-start text-left space-y-6 lg:pr-8"
        >
          <motion.div variants={itemVariants}>
            <p className="text-xs font-bold tracking-[0.2em] text-[var(--foreground)] opacity-70 uppercase">
              AI-POWERED LEGAL EXPERTISE
            </p>
          </motion.div>

          {/* Matches the professional strictness of SOHUPUB */}
          <motion.h1 variants={itemVariants} className="text-4xl md:text-5xl lg:text-6xl font-bold text-[var(--foreground)] leading-[1.1] tracking-tight">
            Navigate Your <br/>
            Civil Law Queries <br/>
            Instantly.
          </motion.h1>

          <motion.p variants={itemVariants} className="max-w-xl text-lg text-[var(--text-muted)] leading-relaxed font-normal mt-2">
            Join a modern community of legal professionals and citizens. Analyze contracts, search complex judgments, and receive instant AI-driven guidance on Pakistani Civil Law.
          </motion.p>

          <motion.div variants={itemVariants} className="flex flex-col sm:flex-row items-center gap-4 mt-8 w-full sm:w-auto">
            {/* The primary button */}
            <Button
              onClick={() => navigate('/auth?mode=register')}
              size="lg"
              className="group w-full sm:w-auto rounded-full bg-[#051326] text-white hover:opacity-90 dark:bg-white dark:text-[#051326] dark:hover:bg-neutral-200 px-8 py-7 text-sm font-bold tracking-wide"
            >
              START RESEARCH <ArrowRight size={16} className="ml-2 group-hover:translate-x-1 transition-transform" />
            </Button>

            {/* The secondary/outline button */}
            <Button
              variant="outline"
              size="lg"
              className="w-full sm:w-auto rounded-full border border-[var(--card-border)] bg-[var(--background)] hover:bg-neutral-50 dark:hover:bg-neutral-800 px-8 py-7 text-sm font-bold tracking-wide text-[var(--foreground)] shadow-sm"
              asChild
            >
              <a href="#features">EXPLORE AI TOOLS</a>
            </Button>
          </motion.div>

        </motion.div>

        {/* Right Column: 2D Scale Illustration Component */}
        <motion.div 
           initial={{ opacity: 0, scale: 0.9 }}
           animate={{ opacity: 1, scale: 1 }}
           transition={{ duration: 1.2, ease: "easeOut", delay: 0.1 }}
           className="relative flex items-center justify-center p-8 lg:p-0 h-[400px] lg:h-[500px] w-full"
        >
          <Suspense fallback={<div className="animate-pulse bg-[var(--card-border)] w-64 h-64 rounded-full"></div>}>
            <Hero2DScale />
          </Suspense>
        </motion.div>
      </div>
    </section>
  );
}