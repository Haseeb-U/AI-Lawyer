import { useState } from 'react';

/**
 * Custom hook for handling form state management seamlessly.
 * Professional way to handle form fields instead of multiple useStates.
 */
export const useForm = (initialValues) => {
    const [values, setValues] = useState(initialValues);

    const handleChange = (e) => {
        const { name, value, type, checked } = e.target;
        setValues({
            ...values,
            [name]: type === 'checkbox' ? checked : value
        });
    };

    const resetForm = () => setValues(initialValues);

    return {
        values,
        handleChange,
        resetForm,
        setValues
    };
};
