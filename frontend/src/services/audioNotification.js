// Audio Notification Service with various sound effects

class AudioNotificationService {
  constructor() {
    this.sounds = {};
    this.isMuted = true; // Disabled by default
    this.volume = 0.5;
    this.initializeSounds();
  }

  // Initialize synthetic sounds using Web Audio API
  initializeSounds() {
    this.createSuccessSound();
    this.createErrorSound();
    this.createWarningSound();
    this.createNotificationSound();
    this.createClickSound();
  }

  // Success - ascending tones
  createSuccessSound() {
    this.sounds.success = async () => {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioContext.currentTime;

      // Create oscillators for beeping sound
      const osc = audioContext.createOscillator();
      const gain = audioContext.createGain();

      osc.connect(gain);
      gain.connect(audioContext.destination);

      // Success sound: two ascending beeps
      osc.frequency.setValueAtTime(600, now);
      gain.gain.setValueAtTime(this.volume, now);
      osc.start(now);
      osc.stop(now + 0.2);

      osc.frequency.setValueAtTime(800, now + 0.1);
      osc.start(now + 0.1);
      osc.stop(now + 0.3);

      gain.gain.exponentialRampToValueAtTime(0.01, now + 0.3);
    };
  }

  // Error - descending tones with buzz
  createErrorSound() {
    this.sounds.error = async () => {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioContext.currentTime;

      const osc = audioContext.createOscillator();
      const gain = audioContext.createGain();

      osc.connect(gain);
      gain.connect(audioContext.destination);

      // Error sound: descending frequency
      osc.frequency.setValueAtTime(400, now);
      gain.gain.setValueAtTime(this.volume, now);
      osc.start(now);

      osc.frequency.exponentialRampToValueAtTime(200, now + 0.4);
      osc.stop(now + 0.4);

      gain.gain.exponentialRampToValueAtTime(0.01, now + 0.4);
    };
  }

  // Warning - repeating middle tone
  createWarningSound() {
    this.sounds.warning = async () => {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioContext.currentTime;

      for (let i = 0; i < 3; i++) {
        const osc = audioContext.createOscillator();
        const gain = audioContext.createGain();

        osc.connect(gain);
        gain.connect(audioContext.destination);

        osc.frequency.setValueAtTime(500, now + i * 0.15);
        gain.gain.setValueAtTime(this.volume, now + i * 0.15);

        osc.start(now + i * 0.15);
        osc.stop(now + i * 0.15 + 0.1);

        gain.gain.exponentialRampToValueAtTime(0.01, now + i * 0.15 + 0.1);
      }
    };
  }

  // Notification - soft double beep
  createNotificationSound() {
    this.sounds.notification = async () => {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioContext.currentTime;

      for (let i = 0; i < 2; i++) {
        const osc = audioContext.createOscillator();
        const gain = audioContext.createGain();

        osc.connect(gain);
        gain.connect(audioContext.destination);

        osc.frequency.setValueAtTime(700, now + i * 0.12);
        gain.gain.setValueAtTime(this.volume * 0.7, now + i * 0.12);

        osc.start(now + i * 0.12);
        osc.stop(now + i * 0.12 + 0.12);

        gain.gain.exponentialRampToValueAtTime(0.01, now + i * 0.12 + 0.12);
      }
    };
  }

  // Click - short beep for button clicks
  createClickSound() {
    this.sounds.click = async () => {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioContext.currentTime;

      const osc = audioContext.createOscillator();
      const gain = audioContext.createGain();

      osc.connect(gain);
      gain.connect(audioContext.destination);

      osc.frequency.setValueAtTime(800, now);
      gain.gain.setValueAtTime(this.volume * 0.5, now);

      osc.start(now);
      osc.stop(now + 0.05);

      gain.gain.exponentialRampToValueAtTime(0.01, now + 0.05);
    };
  }

  // Play a sound effect
  play(soundType = 'notification') {
    if (this.isMuted) return;

    try {
      if (this.sounds[soundType]) {
        this.sounds[soundType]();
      }
    } catch (error) {
      console.warn('Audio notification failed:', error);
    }
  }

  // Play multiple sounds in sequence
  playSequence(soundTypes, delay = 150) {
    soundTypes.forEach((soundType, index) => {
      setTimeout(() => this.play(soundType), index * delay);
    });
  }

  // Mute/unmute
  setMuted(muted) {
    this.isMuted = muted;
    localStorage.setItem('audioMuted', JSON.stringify(muted));
  }

  toggleMute() {
    this.setMuted(!this.isMuted);
    return !this.isMuted;
  }

  // Set volume (0-1)
  setVolume(volume) {
    this.volume = Math.max(0, Math.min(1, volume));
    localStorage.setItem('audioVolume', JSON.stringify(this.volume));
  }

  // Load settings from localStorage
  loadSettings() {
    const muted = localStorage.getItem('audioMuted');
    const volume = localStorage.getItem('audioVolume');

    if (muted) this.isMuted = JSON.parse(muted);
    if (volume) this.volume = JSON.parse(volume);
  }
}

// Create singleton instance
export const audioNotification = new AudioNotificationService();
audioNotification.loadSettings();

// Export for use in components
export default audioNotification;
