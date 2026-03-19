import React from 'react';
import { Gavel, Scale, FileText, Languages, BookOpen, Sparkles } from 'lucide-react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';

export default function FeaturesSection() {
  const coreFeatures = [
    {
      icon: <Gavel size={32} />,
      title: 'Smart Legal Search',
      description: 'Advanced AI-powered semantic search across Pakistani civil laws and regulations with instant, accurate results.'
    },
    {
      icon: <Scale size={32} />,
      title: 'Judgment Analysis',
      description: 'Comprehensive analysis and summarization of Supreme Court and High Court judgments with key findings extraction.'
    },
    {
      icon: <FileText size={32} />,
      title: 'Contract Summarization',
      description: 'Intelligent contract analysis that extracts key terms, obligations, and potential legal implications automatically.'
    },
    {
      icon: <Languages size={32} />,
      title: 'Bilingual Support',
      description: 'Seamless support for both English and Urdu, making legal information accessible to everyone in Pakistan.'
    },
    {
      icon: <BookOpen size={32} />,
      title: 'Civil Law Expertise',
      description: 'Specialized in Pakistani civil law including Contract Act, Property Law, Family Law, and more.'
    },
    {
      icon: <Sparkles size={32} />,
      title: 'AI-Powered Insights',
      description: 'Leveraging cutting-edge AI technology to provide accurate, context-aware legal guidance and recommendations.'
    }
  ];

  return (
    <section id="features" className="py-24 px-4 bg-[var(--background)] smooth-transition text-[var(--foreground)] border-t border-[var(--card-border)] relative overflow-hidden">
      
      {/* Background decoration */}
      <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-brand-500/10 blur-[120px] rounded-full" />
      
      <div className="max-w-7xl mx-auto relative z-10">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.8 }}
          className="text-center mb-16 max-w-3xl mx-auto"
        >
          <div className="inline-block text-brand-400 font-semibold tracking-wider uppercase text-sm mb-4 px-4 py-1 rounded-full border border-brand-500/20 bg-brand-500/10">
            Platform Capabilities
          </div>
          <h2 className="text-4xl md:text-5xl font-bold mb-6">Powerful Features</h2>
          <p className="text-xl text-foreground/70">Everything you need for comprehensive, fast, and accurate legal research in Pakistan.</p>
        </motion.div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {coreFeatures.map((feature, idx) => (
            <motion.div
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-50px" }}
              transition={{ duration: 0.6, delay: idx * 0.1 }}
              key={idx}
              className="h-full"
            >
              <Card className="group h-full hover:border-brand-500/50 hover:shadow-[0_0_40px_rgba(212,175,55,0.15)] hover:-translate-y-2 transition-all duration-500 relative cursor-pointer overflow-hidden border-[var(--card-border)]">
                {/* Feature highlight effect */}
                <div className="absolute inset-0 bg-gradient-to-br from-brand-500/0 via-brand-500/0 to-brand-500/0 group-hover:from-brand-500/5 group-hover:to-transparent transition-all duration-500 z-0" />
                
                <CardHeader className="relative z-10 pt-8 pb-4">
                  <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-neutral-800 to-neutral-900 border border-[var(--card-border)] flex items-center justify-center mb-4 text-brand-400 group-hover:scale-110 group-hover:bg-brand-500/20 group-hover:text-brand-300 transition-all shadow-lg duration-500">
                    {feature.icon}
                  </div>
                  <CardTitle className="text-2xl font-bold group-hover:text-brand-300 transition-colors">
                    {feature.title}
                  </CardTitle>
                </CardHeader>
                <CardContent className="relative z-10 pb-8">
                  <CardDescription className="text-[var(--foreground)]/70 text-base leading-relaxed group-hover:text-[var(--foreground)]/90 transition-colors">
                    {feature.description}
                  </CardDescription>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
