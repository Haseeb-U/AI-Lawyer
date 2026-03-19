import React from 'react';
import { Shield, FileText, Gavel, Scale, BookOpen, Sparkles } from 'lucide-react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/card';

export default function CivilLawCoverage() {
  const practiceAreas = [
    {
      icon: <FileText size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Contract Law',
      topics: ['Contract Act, 1872', 'Sale of Goods Act, 1930', 'Partnership Act, 1932', 'Specific Relief Act, 1877']
    },
    {
      icon: <Gavel size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Property Law',
      topics: ['Transfer of Property Act, 1882', 'Registration Act, 1908', 'Mortgage Laws', 'Lease & Tenancy Rights']
    },
    {
      icon: <Shield size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Family Law',
      topics: ['Muslim Family Laws Ordinance', 'Guardians and Wards Act', 'Succession & Inheritance', 'Marriage & Divorce Laws']
    },
    {
      icon: <Scale size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Tort Law',
      topics: ['Negligence Claims', 'Defamation Laws', 'Personal Injury', 'Damages & Compensation']
    },
    {
      icon: <BookOpen size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Civil Procedure',
      topics: ['Civil Procedure Code, 1908', 'Limitation Act, 1908', 'Court Fees Act, 1870', 'Evidence Act, 1984']
    },
    {
      icon: <Sparkles size={28} className="text-brand-400 group-hover:text-brand-300 transition-colors" />,
      title: 'Consumer Rights',
      topics: ['Consumer Protection Laws', 'Product Liability', 'Service Contracts', 'Dispute Resolution']
    }
  ];

  return (
    <section className="py-24 px-4 bg-[var(--background)] smooth-transition text-[var(--foreground)] relative">
      <div className="max-w-7xl mx-auto">

        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
          className="flex flex-col items-center text-center mb-16 gap-4"
        >
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-brand-500/10 border border-brand-500/20 text-brand-400 text-sm font-medium">
            <Shield size={16} />
            Comprehensive Coverage
          </div>
          <h2 className="text-3xl md:text-5xl font-bold">Specialized in Pakistani Civil Law</h2>
          <p className="max-w-2xl text-[var(--foreground)]/70 text-lg">
            Our AI-powered platform covers all major areas of Civil Law in Pakistan, providing you with accurate legal information and expert guidance.
          </p>
        </motion.div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {practiceAreas.map((area, idx) => (
            <motion.div
              initial={{ opacity: 0, scale: 0.9 }}
              whileInView={{ opacity: 1, scale: 1 }}
              viewport={{ once: true, margin: "-50px" }}
              transition={{ duration: 0.5, delay: idx * 0.1 }}
              key={idx}
              className="h-full"
            >
              <Card className="h-full bg-foreground/5 hover:bg-foreground/10 hover:border-brand-500/30 hover:shadow-[0_0_30px_rgba(212,175,55,0.05)] transition-all duration-300 group cursor-pointer border-[var(--card-border)] rounded-2xl">
                <CardHeader className="pb-2">
                  <div className="w-14 h-14 rounded-xl bg-brand-500/10 flex items-center justify-center mb-4 group-hover:scale-110 group-hover:bg-brand-500/20 transition-all duration-300">
                    {area.icon}
                  </div>
                  <CardTitle className="text-xl group-hover:text-brand-300 transition-colors">
                    {area.title}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    {area.topics.map((topic, i) => (
                      <li key={i} className="text-[var(--foreground)]/70 flex items-start gap-2 group-hover:text-[var(--foreground)]/80 transition-colors">
                        <span className="text-brand-500/50 mt-1">�</span>
                        {topic}
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </div>

      </div>
    </section>
  );
}
