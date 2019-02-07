/* Start an Index scan on pattern (spo) */
CREATE FUNCTION sage_scan_spo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND predicate = $3 AND object = $4;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (???) */
CREATE FUNCTION sage_scan_vvv(refcursor) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (???) */
CREATE FUNCTION sage_resume_vvv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE (subject, predicate, object) >= ($2, $3, $4);
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (s??) */
CREATE FUNCTION sage_scan_svv(refcursor, subj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (s??) */
CREATE FUNCTION sage_resume_svv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND (predicate, object) >= ($3, $4);
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (?p?) */
CREATE FUNCTION sage_scan_vpv(refcursor, pred text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE predicate = $2 ORDER BY subject, object;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (?p?) */
CREATE FUNCTION sage_resume_vpv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE predicate = $3 AND (subject, object) >= ($2, $4) ORDER BY subject, object;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (??o) */
CREATE FUNCTION sage_scan_vvo(refcursor, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE object = $2 ORDER BY subject, predicate;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (??o) */
CREATE FUNCTION sage_resume_vvo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE object = $4 AND (subject, predicate) >= ($2, $3) ORDER BY subject, predicate;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (sp?) */
CREATE FUNCTION sage_scan_spv(refcursor, subj text, pred text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND predicate = $3;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (sp?) */
CREATE FUNCTION sage_resume_spv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND predicate = $3 AND (object) >= ($4);
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (?po) */
CREATE FUNCTION sage_scan_vpo(refcursor, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE predicate = $2 AND object = $3;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (?po) */
CREATE FUNCTION sage_resume_vpo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE predicate = $3 AND object = $4 AND (subject) >= ($2);
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Start an Index scan on pattern (s?o) */
CREATE FUNCTION sage_scan_svo(refcursor, subj text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND object = $3;
    RETURN $1;
END;
$$ LANGUAGE plpgsql;

/* Resume an Index scan on pattern (s?o) */
CREATE FUNCTION sage_resume_svo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM sage_spo_table WHERE subject = $2 AND object = $4 AND (predicate) >= ($3);
    RETURN $1;
END;
$$ LANGUAGE plpgsql;
